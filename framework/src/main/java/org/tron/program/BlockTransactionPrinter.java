package org.tron.program;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.tron.common.application.Application;
import org.tron.common.application.ApplicationFactory;
import org.tron.common.application.TronApplicationContext;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.StringUtil;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.ChainBaseManager;
import org.tron.core.Constant;
import org.tron.core.Wallet;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.core.services.http.JsonFormat;
import org.tron.core.services.http.Util;
import org.tron.protos.Protocol.Transaction;
import org.tron.protos.Protocol.Transaction.Contract.ContractType;
import org.tron.protos.Protocol.Transaction.Result;
import org.tron.protos.Protocol.TransactionInfo;
import org.tron.protos.Protocol.TransactionInfo.Log;
import org.tron.protos.Protocol.InternalTransaction;
import org.tron.protos.Protocol.ResourceReceipt;
import org.tron.protos.contract.BalanceContract.TransferContract;
import org.tron.protos.contract.AssetIssueContractOuterClass;
import org.tron.protos.contract.SmartContractOuterClass;

// TransactionLogTrigger related imports
import org.tron.common.logsfilter.trigger.TransactionLogTrigger;
import org.tron.common.logsfilter.trigger.InternalTransactionPojo;
import org.tron.common.logsfilter.trigger.LogPojo;
import org.tron.common.utils.JsonUtil;
import org.tron.common.utils.StringUtil;
import org.bouncycastle.util.encoders.Hex;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// Kafka imports
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * A test program to print transactions from a specified block range.
 * 
 * This program connects to the TRON blockchain, retrieves blocks within the specified range,
 * and prints detailed information about all transactions in those blocks.
 * 
 * Usage:
 * 1. Compile the TRON project:
 *    ./gradlew build -x test
 * 
 * 2. Run the program with start and end block numbers:
 *    java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter <startBlockNum> <endBlockNum> [options]
 * 
 * Examples:
 *    java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100
 *    java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 73592989 73592991 -c main_net_config.conf -d /tron/light/
 *    java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -fm trigger
 *    java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -fm json
 *    java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -fm trigger -kb localhost:9092 -kt tron-transactions
 *    java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter -tx <txid> -fm trigger -kb localhost:9092 -kt tron-transactions
 *    java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -fm trigger -threads 8
 * 
 * Options:
 *    -c <config_file>: Specify a custom configuration file
 *    -d <data_dir>: Specify a custom data directory
 *    -fm <format>: Output format, default: both
 *        json     - Standard JSON format using JsonFormat
 *        protobuf - Protobuf toString format
 *        both     - Both JSON and protobuf formats
 *        trigger  - TransactionLogTrigger format (structured JSON with comprehensive transaction data)
 *    -kb <kafka_brokers>: Kafka broker addresses (e.g., localhost:9092,broker2:9092)
 *    -kt <kafka_topic>: Kafka topic name for sending trigger data
 *    -threads <count>: Number of threads for concurrent transaction processing within each block (default: CPU cores * 2)
 *    (Other standard TRON node options are also supported)
 * 
 * The program will:
 * - Connect to the TRON blockchain using the specified configuration
 * - Retrieve blocks in the specified range (from startBlockNum to endBlockNum)
 * - Process transactions within each block concurrently for improved performance
 * - Print detailed information about each block and its transactions
 * - Show a summary of the total blocks and transactions processed with performance statistics
 * 
 * Note: Make sure you have a running TRON node or a valid database directory
 * configured to access the blockchain data.
 */
@Slf4j(topic = "app")
public class BlockTransactionPrinter {

  private static KafkaProducer<String, String> kafkaProducer = null;

  // Concurrent processing configuration
  private static ExecutorService transactionExecutor = null;
  private static final int DEFAULT_THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;
  private static int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;

  // Statistics tracking
  private static long startTime = 0;
  private static long totalBlocksProcessed = 0;
  private static long totalTransactionsProcessed = 0;
  private static long lastStatsTime = 0;
  private static long lastBlocksProcessed = 0;
  private static long lastTransactionsProcessed = 0;
  private static long startBlockNumber = 0;
  private static long currentBlockNumber = 0;

  /**
   * Initialize thread pool for concurrent transaction processing
   */
  private static void initializeThreadPool(int poolSize) {
    if (transactionExecutor != null) {
      return; // Already initialized
    }

    threadPoolSize = poolSize;
    transactionExecutor = Executors.newFixedThreadPool(threadPoolSize);
    String threadPoolMessage = "Transaction processing thread pool initialized with " + threadPoolSize + " threads";
    logger.info(threadPoolMessage);
  }

  /**
   * Shutdown thread pool gracefully
   */
  private static void shutdownThreadPool() {
    if (transactionExecutor != null) {
      try {
        logger.debug("Shutting down transaction processing thread pool...");
        transactionExecutor.shutdown();

        // Wait for existing tasks to complete
        if (!transactionExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
          logger.warn("Thread pool did not terminate gracefully, forcing shutdown...");
          transactionExecutor.shutdownNow();

          // Wait a bit more for tasks to respond to being cancelled
          if (!transactionExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
            logger.error("Thread pool did not terminate after forced shutdown");
          }
        }

        logger.info("Transaction processing thread pool shut down successfully");
      } catch (InterruptedException e) {
        logger.error("Thread pool shutdown interrupted", e);
        transactionExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      } finally {
        transactionExecutor = null;
      }
    }
  }

  /**
   * Initialize statistics tracking
   */
  private static void initializeStatistics(long startBlock) {
    startTime = System.currentTimeMillis();
    lastStatsTime = startTime;
    totalBlocksProcessed = 0;
    totalTransactionsProcessed = 0;
    lastBlocksProcessed = 0;
    lastTransactionsProcessed = 0;
    startBlockNumber = startBlock;
    currentBlockNumber = startBlock;
    logger.info("Statistics tracking initialized - Starting from block {}", startBlock);
  }

  /**
   * Update and log statistics
   */
  private static void updateStatistics(int blocksProcessed, int transactionsProcessed, long latestBlockNumber) {
    totalBlocksProcessed += blocksProcessed;
    totalTransactionsProcessed += transactionsProcessed;
    currentBlockNumber = latestBlockNumber;

    long currentTime = System.currentTimeMillis();
    long timeSinceLastStats = currentTime - lastStatsTime;

    // Log statistics every 10 seconds or every 100 blocks
    if (timeSinceLastStats >= 10000 || totalBlocksProcessed - lastBlocksProcessed >= 100) {
      long totalTime = currentTime - startTime;
      double totalTimeSeconds = totalTime / 1000.0;
      double intervalTimeSeconds = timeSinceLastStats / 1000.0;

      // Calculate overall rates
      double overallBlocksPerSecond = totalTimeSeconds > 0 ? totalBlocksProcessed / totalTimeSeconds : 0;
      double overallTransactionsPerSecond = totalTimeSeconds > 0 ? totalTransactionsProcessed / totalTimeSeconds : 0;

      // Calculate interval rates
      long intervalBlocks = totalBlocksProcessed - lastBlocksProcessed;
      long intervalTransactions = totalTransactionsProcessed - lastTransactionsProcessed;
      double intervalBlocksPerSecond = intervalTimeSeconds > 0 ? intervalBlocks / intervalTimeSeconds : 0;
      double intervalTransactionsPerSecond = intervalTimeSeconds > 0 ? intervalTransactions / intervalTimeSeconds : 0;

      String statsMessage = String.format(
        "Processing Statistics - Current Block: %d (Started from: %d) | Total: %d blocks, %d transactions in %.1fs " +
        "(%.2f blocks/s, %.2f tx/s) | Recent: %d blocks, %d transactions in %.1fs " +
        "(%.2f blocks/s, %.2f tx/s)",
        currentBlockNumber, startBlockNumber,
        totalBlocksProcessed, totalTransactionsProcessed, totalTimeSeconds,
        overallBlocksPerSecond, overallTransactionsPerSecond,
        intervalBlocks, intervalTransactions, intervalTimeSeconds,
        intervalBlocksPerSecond, intervalTransactionsPerSecond
      );

      System.out.println("=== " + statsMessage + " ===");
      logger.info(statsMessage);

      // Update last stats tracking
      lastStatsTime = currentTime;
      lastBlocksProcessed = totalBlocksProcessed;
      lastTransactionsProcessed = totalTransactionsProcessed;
    }
  }

  /**
   * Log final statistics
   */
  private static void logFinalStatistics() {
    long totalTime = System.currentTimeMillis() - startTime;
    double totalTimeSeconds = totalTime / 1000.0;
    double overallBlocksPerSecond = totalTimeSeconds > 0 ? totalBlocksProcessed / totalTimeSeconds : 0;
    double overallTransactionsPerSecond = totalTimeSeconds > 0 ? totalTransactionsProcessed / totalTimeSeconds : 0;

    String finalStatsMessage = String.format(
      "Final Statistics - Block Range: %d to %d | Processed %d blocks and %d transactions in %.1f seconds " +
      "(Average: %.2f blocks/s, %.2f transactions/s)",
      startBlockNumber, currentBlockNumber,
      totalBlocksProcessed, totalTransactionsProcessed, totalTimeSeconds,
      overallBlocksPerSecond, overallTransactionsPerSecond
    );

    System.out.println("=== " + finalStatsMessage + " ===");
    logger.info(finalStatsMessage);
  }

  /**
   * Initialize Kafka producer
   */
  private static void initKafkaProducer(String kafkaBrokers) {
    if (kafkaProducer != null) {
      return; // Already initialized
    }

    try {
      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

      // Producer configuration for reliability
      props.put(ProducerConfig.ACKS_CONFIG, "1"); // Wait for leader acknowledgment
      props.put(ProducerConfig.RETRIES_CONFIG, 3);
      props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
      props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
      props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

      // Enable gzip compression for better network efficiency
      props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

      kafkaProducer = new KafkaProducer<>(props);
      String kafkaInitMessage = "Kafka producer initialized successfully with brokers: " + kafkaBrokers;
      logger.info(kafkaInitMessage);
    } catch (Exception e) {
      String kafkaErrorMessage = "Failed to initialize Kafka producer: " + e.getMessage();
      logger.error(kafkaErrorMessage, e);
      kafkaProducer = null;
    }
  }

  /**
   * Send message to Kafka topic
   */
  private static void sendToKafka(String topic, String key, String message) {
    if (kafkaProducer == null) {
      String errorMessage = "Kafka producer not initialized. Cannot send message.";
      System.err.println(errorMessage);
      logger.warn(errorMessage);
      return;
    }

    try {
      ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
      kafkaProducer.send(record, (metadata, exception) -> {
        if (exception != null) {
          String errorMessage = "Failed to send message to Kafka: " + exception.getMessage();
          logger.error(errorMessage, exception);
        } else {
          // logger.debug("Message sent to Kafka topic '{}' at offset {}", topic, metadata.offset());
        }
      });
    } catch (Exception e) {
      String errorMessage = "Error sending message to Kafka: " + e.getMessage();
      System.err.println(errorMessage);
      logger.error(errorMessage, e);
      e.printStackTrace();
    }
  }

  /**
   * Close Kafka producer
   */
  private static void closeKafkaProducer() {
    if (kafkaProducer != null) {
      try {
        kafkaProducer.flush(); // Ensure all messages are sent
        kafkaProducer.close();
        logger.info("Kafka producer closed successfully");
      } catch (Exception e) {
        logger.error("Error closing Kafka producer: " + e.getMessage(), e);
      } finally {
        kafkaProducer = null;
      }
    }
  }

  /**
   * Print transaction info in the specified format
   */
  private static void printTransactionInfo(TransactionInfo transactionInfo, String outputFormat, String title) {
    if (transactionInfo == null) {
      System.out.println("No transaction info available");
      return;
    }

    // Convert log addresses to TRON addresses while preserving internal transactions
    List<Log> newLogList = Util.convertLogAddressToTronAddress(transactionInfo);
    TransactionInfo transactionInfoWithConvertedLogs = transactionInfo.toBuilder()
        .clearLog()
        .addAllLog(newLogList)
        .build();

    // Removed console output for transaction title to reduce noise
    // System.out.println("=== " + title + " ===");

    if ("json".equals(outputFormat) || "both".equals(outputFormat)) {
      System.out.println("--- JSON Format ---");
      System.out.println(JsonFormat.printToString(transactionInfoWithConvertedLogs, true));
    }

    if ("protobuf".equals(outputFormat) || "both".equals(outputFormat)) {
      System.out.println("--- Protobuf Format ---");
      System.out.println(transactionInfoWithConvertedLogs.toString());
    }
  }

  /**
   * Print transaction in TransactionLogTrigger format
   */
  private static void printTransactionLogTrigger(TransactionInfo transactionInfo, Transaction transaction,
      String blockHash, long blockNumber, long timestamp, int transactionIndex, String title) {
    printTransactionLogTrigger(transactionInfo, transaction, blockHash, blockNumber, timestamp, transactionIndex, title, null, null, null);
  }

  /**
   * Print transaction in TransactionLogTrigger format with TransactionCapsule
   */
  private static void printTransactionLogTrigger(TransactionInfo transactionInfo, Transaction transaction,
      String blockHash, long blockNumber, long timestamp, int transactionIndex, String title, TransactionCapsule trxCapsule) {
    printTransactionLogTrigger(transactionInfo, transaction, blockHash, blockNumber, timestamp, transactionIndex, title, null, null, trxCapsule);
  }

  /**
   * Print transaction in TransactionLogTrigger format with optional Kafka sending
   */
  private static void printTransactionLogTrigger(TransactionInfo transactionInfo, Transaction transaction,
      String blockHash, long blockNumber, long timestamp, int transactionIndex, String title,
      String kafkaTopic, String kafkaKey) {
    printTransactionLogTrigger(transactionInfo, transaction, blockHash, blockNumber, timestamp, transactionIndex, title, kafkaTopic, kafkaKey, null);
  }

  /**
   * Print transaction in TransactionLogTrigger format with optional Kafka sending and TransactionCapsule
   */
  private static void printTransactionLogTrigger(TransactionInfo transactionInfo, Transaction transaction,
      String blockHash, long blockNumber, long timestamp, int transactionIndex, String title,
      String kafkaTopic, String kafkaKey, TransactionCapsule trxCapsule) {

    // Removed console output for transaction title to reduce noise
    // System.out.println("=== " + title + " ===");

    // Check if we have valid transaction data before proceeding
    if (transactionInfo == null && transaction == null) {
      logger.warn("Attempted to create TransactionLogTrigger with null transactionInfo and transaction");
      return;
    }

    // For genesis block transactions, we might only have transaction data without transactionInfo
    if (transactionInfo == null && transaction != null) {
      // logger.debug("Creating TransactionLogTrigger from transaction data only for block {}", blockNumber);
    }

    try {
      TransactionLogTrigger trigger = createTransactionLogTrigger(
          transactionInfo, transaction, blockHash, blockNumber, timestamp, transactionIndex, trxCapsule);

      String jsonOutput = JsonUtil.obj2Json(trigger);
      if (jsonOutput != null) {
        boolean sentToKafka = false;

        // Send to Kafka if configured and we have valid transaction data
        if (kafkaTopic != null && kafkaProducer != null) {
          String key = kafkaKey != null ? kafkaKey : trigger.getTransactionId();
          // Additional check to ensure we don't send invalid transaction IDs
          // Accept calculated IDs for genesis transactions
          if (trigger.getTransactionId() != null &&
              !"N/A".equals(trigger.getTransactionId()) &&
              !trigger.getTransactionId().isEmpty() &&
              !trigger.getTransactionId().startsWith("UNKNOWN_TX_")) {
            sendToKafka(kafkaTopic, key, jsonOutput);
            // logger.debug("TransactionLogTrigger sent to Kafka topic: {} with key: {}", kafkaTopic, key);
            sentToKafka = true;
          } else {
            logger.warn("Skipped Kafka send due to invalid transaction ID: {}", trigger.getTransactionId());
          }
        }

        // Only print to console if not sent to Kafka
        if (!sentToKafka) {
          System.out.println(jsonOutput);
        }
      } else {
        logger.error("Failed to serialize TransactionLogTrigger to JSON for transaction");
      }
    } catch (Exception e) {
      logger.error("Error creating TransactionLogTrigger: " + e.getMessage(), e);
    }
  }

  /**
   * Print transaction in the specified format
   */
  private static void printTransaction(Transaction transaction, String outputFormat, String title) {
    if (transaction == null) {
      System.out.println("No transaction available");
      return;
    }

    // Removed console output for transaction title to reduce noise
    // System.out.println("=== " + title + " ===");

    if ("trigger".equals(outputFormat)) {
      System.out.println("Warning: TransactionLogTrigger format requires both TransactionInfo and Transaction data.");
      System.out.println("Use printTransactionLogTrigger() method instead for complete trigger format support.");
      System.out.println("Falling back to JSON format:");
      System.out.println(JsonFormat.printToString(transaction, true));
      return;
    }

    if ("json".equals(outputFormat) || "both".equals(outputFormat)) {
      System.out.println("--- JSON Format ---");
      System.out.println(JsonFormat.printToString(transaction, true));
    }

    if ("protobuf".equals(outputFormat) || "both".equals(outputFormat)) {
      System.out.println("--- Protobuf Format ---");
      System.out.println(transaction.toString());
    }
  }

  /**
   * Create TransactionLogTrigger from TransactionInfo and Transaction data
   */
  private static TransactionLogTrigger createTransactionLogTrigger(
      TransactionInfo transactionInfo, Transaction transaction,
      String blockHash, long blockNumber, long timestamp, int transactionIndex) {
    return createTransactionLogTrigger(transactionInfo, transaction, blockHash, blockNumber, timestamp, transactionIndex, null);
  }

  /**
   * Create TransactionLogTrigger from TransactionInfo and Transaction data with optional TransactionCapsule
   */
  private static TransactionLogTrigger createTransactionLogTrigger(
      TransactionInfo transactionInfo, Transaction transaction,
      String blockHash, long blockNumber, long timestamp, int transactionIndex, TransactionCapsule trxCapsule) {

    TransactionLogTrigger trigger = new TransactionLogTrigger();

    // Basic transaction information - prioritize TransactionCapsule ID if available
    if (trxCapsule != null) {
      try {
        String trxCapsuleId = trxCapsule.getTransactionId().toString();
        trigger.setTransactionId(trxCapsuleId);
        logger.debug("Using transaction ID from TransactionCapsule: {}", trxCapsuleId);
      } catch (Exception e) {
        logger.debug("Could not get transaction ID from TransactionCapsule: {}", e.getMessage());
        // Fall through to other methods
      }
    }

    // If we don't have ID from TransactionCapsule, try TransactionInfo
    if (trigger.getTransactionId() == null && transactionInfo != null) {
      trigger.setTransactionId(Hex.toHexString(transactionInfo.getId().toByteArray()));
      logger.debug("Using transaction ID from TransactionInfo");
    }

    // If still no ID, calculate from transaction data
    if (trigger.getTransactionId() == null && transaction != null) {
      try {
        // Use the correct TRON transaction ID calculation: hash of RawData
        byte[] rawDataBytes = transaction.getRawData().toByteArray();
        Sha256Hash txHash = Sha256Hash.of(CommonParameter.getInstance().isECKeyCryptoEngine(), rawDataBytes);
        String calculatedTxId = txHash.toString();
        trigger.setTransactionId(calculatedTxId);
        logger.debug("Calculated transaction ID from RawData: {}", calculatedTxId);
      } catch (Exception e) {
        trigger.setTransactionId("GENESIS_TX_" + transactionIndex);
        logger.debug("Using fallback transaction ID: GENESIS_TX_{}, Error: {}", transactionIndex, e.getMessage());
      }
    }

    // Last resort: use a placeholder ID
    if (trigger.getTransactionId() == null) {
      trigger.setTransactionId("UNKNOWN_TX_" + transactionIndex);
      logger.debug("Using unknown transaction ID placeholder");
    }

    trigger.setBlockHash(blockHash);
    trigger.setBlockNumber(blockNumber);
    trigger.setTimeStamp(timestamp);
    trigger.setTransactionIndex(transactionIndex);

    // Transaction data
    if (transaction != null && transaction.getRawData() != null) {
      trigger.setData(Hex.toHexString(transaction.getRawData().getData().toByteArray()));
      trigger.setFeeLimit(transaction.getRawData().getFeeLimit());

      // Initialize extMap if not exists
      if (trigger.getExtMap() == null) {
        trigger.setExtMap(new HashMap<>());
      }

      trigger.setTransactionDetail(transaction.toString());

      // Also store in extMap for backward compatibility
      trigger.getExtMap().put("refBlockNum", transaction.getRawData().getRefBlockNum());
      trigger.getExtMap().put("expiration", transaction.getRawData().getExpiration());
      trigger.getExtMap().put("timestamp", transaction.getRawData().getTimestamp());

      // Authority information (auths field) - contains permission information
      // Note: auths field is rarely used in normal transactions, most permission info is in contract.Permission_id
      try {
        // Try to access auths field, but don't fail if it's not available
        java.lang.reflect.Method getAuthsCountMethod = transaction.getRawData().getClass().getMethod("getAuthsCount");
        int authsCount = (Integer) getAuthsCountMethod.invoke(transaction.getRawData());

        if (authsCount > 0) {
          trigger.getExtMap().put("authsCount", (long) authsCount);
          logger.debug("Transaction {} has {} auths entries", trigger.getTransactionId(), authsCount);

          // If auths exist, try to process them
          java.lang.reflect.Method getAuthsMethod = transaction.getRawData().getClass().getMethod("getAuths", int.class);
          for (int i = 0; i < authsCount; i++) {
            Object auth = getAuthsMethod.invoke(transaction.getRawData(), i);
            trigger.getExtMap().put("auth_" + i + "_exists", 1L);
            logger.debug("Transaction {} - auth_{} exists", trigger.getTransactionId(), i);
          }
        } else {
          logger.debug("Transaction {} has no auths field data", trigger.getTransactionId());
        }
      } catch (Exception e) {
        // auths field might not be available in this TRON version or protobuf definition
        logger.debug("Transaction {} - auths field not available or accessible: {}", trigger.getTransactionId(), e.getMessage());
      }

      // Contract information
      if (transaction.getRawData().getContractCount() > 0) {
        Transaction.Contract contract = transaction.getRawData().getContract(0);
        trigger.setContractType(contract.getType().toString());
        trigger.setContractCallValue(getCallValue(contract));
        trigger.setContractData(contract.getParameter().toString());

        // Permission_id is already in the transaction object
        if (contract.getPermissionId() > 0) {
          logger.debug("Transaction {} - Permission_id: {}", trigger.getTransactionId(), contract.getPermissionId());
        }

        // Extract transfer information for transfer contracts
        extractTransferInfo(trigger, contract);
      }
    }

    // Transaction signatures - add to TransactionDetail
    if (transaction != null && transaction.getSignatureCount() > 0) {
      if (trigger.getExtMap() == null) {
        trigger.setExtMap(new HashMap<>());
      }

      trigger.getExtMap().put("signatureCount", (long) transaction.getSignatureCount());

      List<String> signatures = new ArrayList<>();
      for (int i = 0; i < transaction.getSignatureCount(); i++) {
        String signature = Hex.toHexString(transaction.getSignature(i).toByteArray());
        signatures.add(signature);

        // Log the actual signature for debugging
        logger.debug("Transaction {} - signature_{}: {}", trigger.getTransactionId(), i, signature);
      }

      // Signatures are already in the transaction object
      logger.debug("Transaction {} - Added {} signatures to TransactionDetail", trigger.getTransactionId(), signatures.size());
    }

    // Transaction execution results and fees
    if (transactionInfo != null) {
      // Result information
      if (transactionInfo.getResult() != TransactionInfo.code.SUCESS) {
        trigger.setResult(transactionInfo.getResult().toString());
        trigger.setTxResult(transactionInfo.getResult().toString());
      } else {
        trigger.setResult("SUCCESS");
        trigger.setTxResult("SUCCESS");
      }

      // Total fee
      trigger.setFee(transactionInfo.getFee());

      // Use protobuf encoding for txResult - create a Transaction.Result structure
      try {
        Transaction.Result.Builder resultBuilder = Transaction.Result.newBuilder();

        // Set basic fields
        resultBuilder.setFee(transactionInfo.getFee());

        // Map TransactionInfo.code to Transaction.Result.code
        if (transactionInfo.getResult() == TransactionInfo.code.SUCESS) {
          resultBuilder.setRet(Transaction.Result.code.SUCESS);
        } else {
          resultBuilder.setRet(Transaction.Result.code.FAILED);
        }

        // Set default contract result
        resultBuilder.setContractRet(Transaction.Result.contractResult.SUCCESS);

        // Add withdraw and unfreeze amounts if available
        if (transactionInfo.getWithdrawAmount() > 0) {
          resultBuilder.setWithdrawAmount(transactionInfo.getWithdrawAmount());
        }
        if (transactionInfo.getUnfreezeAmount() > 0) {
          resultBuilder.setUnfreezeAmount(transactionInfo.getUnfreezeAmount());
        }

        // Add exchange-related amounts if available
        if (transactionInfo.getExchangeReceivedAmount() > 0) {
          resultBuilder.setExchangeReceivedAmount(transactionInfo.getExchangeReceivedAmount());
        }
        if (transactionInfo.getExchangeInjectAnotherAmount() > 0) {
          resultBuilder.setExchangeInjectAnotherAmount(transactionInfo.getExchangeInjectAnotherAmount());
        }
        if (transactionInfo.getExchangeWithdrawAnotherAmount() > 0) {
          resultBuilder.setExchangeWithdrawAnotherAmount(transactionInfo.getExchangeWithdrawAnotherAmount());
        }
        if (transactionInfo.getExchangeId() > 0) {
          resultBuilder.setExchangeId(transactionInfo.getExchangeId());
        }

        // Add asset issue ID if available
        if (!transactionInfo.getAssetIssueID().isEmpty()) {
          resultBuilder.setAssetIssueID(transactionInfo.getAssetIssueID());
        }

        // Add shielded transaction fee if available
        if (transactionInfo.getShieldedTransactionFee() > 0) {
          resultBuilder.setShieldedTransactionFee(transactionInfo.getShieldedTransactionFee());
        }

        // Build the protobuf result and use its toString() method for structured output
        Transaction.Result txResult = resultBuilder.build();
        trigger.setTxResult(txResult.toString());

      } catch (Exception e) {
        logger.warn("Failed to create protobuf Transaction.Result, falling back to simple format: {}", e.getMessage());
        // Fallback to simple format if protobuf creation fails
        trigger.setTxResult("fee:" + transactionInfo.getFee() + ",result:" + transactionInfo.getResult().toString());
      }
    }

    // Note: Transaction.Result is not available in database-stored transactions (getRetCount() = 0)
    // All execution result information is available in TransactionInfo instead

    if (transactionInfo != null) {

      // Fee and energy information
      if (transactionInfo.hasReceipt()) {
        ResourceReceipt receipt = transactionInfo.getReceipt();
        trigger.setEnergyUsage(receipt.getEnergyUsage());
        trigger.setEnergyFee(receipt.getEnergyFee());
        trigger.setOriginEnergyUsage(receipt.getOriginEnergyUsage());
        trigger.setEnergyUsageTotal(receipt.getEnergyUsageTotal());
        trigger.setNetUsage(receipt.getNetUsage());
        trigger.setNetFee(receipt.getNetFee());

        // Energy penalty total - available in ResourceReceipt
        // Note: This field might not be available in all TRON versions
        // trigger.setEnergyPenaltyTotal(receipt.getEnergyPenaltyTotal());
      }

      // Contract result
      if (transactionInfo.getContractResultCount() > 0) {
        trigger.setContractResult(Hex.toHexString(transactionInfo.getContractResult(0).toByteArray()));
      }

      // Contract address
      if (transactionInfo.getContractAddress() != null && !transactionInfo.getContractAddress().isEmpty()) {
        trigger.setContractAddress(StringUtil.encode58Check(transactionInfo.getContractAddress().toByteArray()));
      }

      // Internal transactions
      if (transactionInfo.getInternalTransactionsCount() > 0) {
        List<InternalTransactionPojo> internalTxList = new ArrayList<>();
        for (InternalTransaction internalTx : transactionInfo.getInternalTransactionsList()) {
          InternalTransactionPojo pojo = new InternalTransactionPojo();
          pojo.setHash(Hex.toHexString(internalTx.getHash().toByteArray()));

          // Handle CallValueInfo
          if (internalTx.getCallValueInfoCount() > 0) {
            InternalTransaction.CallValueInfo callValueInfo = internalTx.getCallValueInfo(0);
            pojo.setCallValue(callValueInfo.getCallValue());
            if (!callValueInfo.getTokenId().isEmpty()) {
              pojo.getTokenInfo().put(callValueInfo.getTokenId(), callValueInfo.getCallValue());
            }
          }

          pojo.setCaller_address(StringUtil.encode58Check(internalTx.getCallerAddress().toByteArray()));
          pojo.setTransferTo_address(StringUtil.encode58Check(internalTx.getTransferToAddress().toByteArray()));

          // Handle extra data
          if (!internalTx.getExtra().isEmpty()) {
            pojo.setData(internalTx.getExtra());
          }

          pojo.setRejected(internalTx.getRejected());
          pojo.setNote(internalTx.getNote().toStringUtf8());
          internalTxList.add(pojo);
        }
        trigger.setInternalTransactionList(internalTxList);
      }

      // Logs
      if (transactionInfo.getLogCount() > 0) {
        List<LogPojo> logList = new ArrayList<>();
        for (int i = 0; i < transactionInfo.getLogCount(); i++) {
          TransactionInfo.Log log = transactionInfo.getLog(i);
          LogPojo logPojo = new LogPojo();

          logPojo.setAddress(Hex.toHexString(log.getAddress().toByteArray()));
          logPojo.setBlockHash(blockHash);
          logPojo.setBlockNumber(blockNumber);
          logPojo.setData(Hex.toHexString(log.getData().toByteArray()));
          logPojo.setLogIndex(i);
          logPojo.setTransactionHash(trigger.getTransactionId());
          logPojo.setTransactionIndex(transactionIndex);

          // Topics
          List<String> topics = new ArrayList<>();
          for (int j = 0; j < log.getTopicsCount(); j++) {
            topics.add(Hex.toHexString(log.getTopics(j).toByteArray()));
          }
          logPojo.setTopicList(topics);

          logList.add(logPojo);
        }
        trigger.setLogList(logList);
      }

      // Additional fields that were missing
      // Note: Some of these fields may not be directly available in TransactionInfo
      // and might need to be calculated or retrieved from other sources

      // Memo fee and multi-sign fee - these are available in ResourceReceipt but not exposed in protobuf
      // Setting to 0 as they're not directly available in the public TransactionInfo API
      trigger.setMemoFee(0);
      trigger.setMultiSignFee(0);

      // Energy unit price - this might need to be retrieved from chain parameters
      // For now, setting to 0 as it's not directly available in TransactionInfo
      trigger.setEnergyUnitPrice(0);

      // Cumulative energy used - this would typically be calculated across multiple transactions
      // For a single transaction, it's the same as energyUsageTotal
      if (transactionInfo.hasReceipt()) {
        trigger.setCumulativeEnergyUsed(transactionInfo.getReceipt().getEnergyUsageTotal());
      }

      // Pre-cumulative log count - this would be the log count before this transaction
      // For now, setting to 0 as it requires context of previous transactions
      trigger.setPreCumulativeLogCount(0);

      // Latest solidified block number - this would need to be retrieved from the chain
      // For now, using the current block number as a placeholder
      trigger.setLatestSolidifiedBlockNumber(blockNumber);

      // ExtMap - merge getCancelUnfreezeV2AmountMap() from TransactionInfo with existing extMap data
      // IMPORTANT: Don't overwrite existing extMap, merge the data instead
      if (trigger.getExtMap() == null) {
        trigger.setExtMap(new HashMap<>());
      }

      if (transactionInfo.getCancelUnfreezeV2AmountCount() > 0) {
        // Merge TransactionInfo's getCancelUnfreezeV2AmountMap() with existing extMap
        Map<String, Long> cancelUnfreezeMap = transactionInfo.getCancelUnfreezeV2AmountMap();
        trigger.getExtMap().putAll(cancelUnfreezeMap);
        logger.debug("Transaction {} - Added {} cancel unfreeze entries to extMap",
                    trigger.getTransactionId(), cancelUnfreezeMap.size());
      }

      // Additional TransactionInfo fields that can be extracted
      // These fields are available in TransactionInfo but not commonly used in TransactionLogTrigger
      // However, they could be useful for specific contract types

      // Asset-related fields
      if (!transactionInfo.getAssetIssueID().isEmpty()) {
        // This could be stored in extMap or used to set assetName for asset issue transactions
        trigger.getExtMap().put("assetIssueID", Long.parseLong(transactionInfo.getAssetIssueID()));
      }

      // Withdraw and unfreeze amounts
      if (transactionInfo.getWithdrawAmount() > 0) {
        trigger.getExtMap().put("withdrawAmount", transactionInfo.getWithdrawAmount());
      }

      if (transactionInfo.getUnfreezeAmount() > 0) {
        trigger.getExtMap().put("unfreezeAmount", transactionInfo.getUnfreezeAmount());
      }

      if (transactionInfo.getWithdrawExpireAmount() > 0) {
        trigger.getExtMap().put("withdrawExpireAmount", transactionInfo.getWithdrawExpireAmount());
      }

      // Exchange-related fields
      if (transactionInfo.getExchangeId() > 0) {
        trigger.getExtMap().put("exchangeId", transactionInfo.getExchangeId());
        if (transactionInfo.getExchangeReceivedAmount() > 0) {
          trigger.getExtMap().put("exchangeReceivedAmount", transactionInfo.getExchangeReceivedAmount());
        }
        if (transactionInfo.getExchangeInjectAnotherAmount() > 0) {
          trigger.getExtMap().put("exchangeInjectAnotherAmount", transactionInfo.getExchangeInjectAnotherAmount());
        }
        if (transactionInfo.getExchangeWithdrawAnotherAmount() > 0) {
          trigger.getExtMap().put("exchangeWithdrawAnotherAmount", transactionInfo.getExchangeWithdrawAnotherAmount());
        }
      }

      // Shielded transaction fee
      if (transactionInfo.getShieldedTransactionFee() > 0) {
        trigger.getExtMap().put("shieldedTransactionFee", transactionInfo.getShieldedTransactionFee());
      }

      // Packing fee
      if (transactionInfo.getPackingFee() > 0) {
        trigger.getExtMap().put("packingFee", transactionInfo.getPackingFee());
      }
    }

    // Log final extMap contents for debugging
    if (trigger.getExtMap() != null && !trigger.getExtMap().isEmpty()) {
      logger.debug("Transaction {} - Final extMap contents: {}", trigger.getTransactionId(), trigger.getExtMap());
    } else {
      logger.warn("Transaction {} - extMap is null or empty!", trigger.getTransactionId());
    }

    return trigger;
  }

  /**
   * Extract call value from contract
   */
  private static long getCallValue(Transaction.Contract contract) {
    try {
      switch (contract.getType()) {
        case TransferContract:
          TransferContract transferContract = contract.getParameter().unpack(TransferContract.class);
          return transferContract.getAmount();
        case TriggerSmartContract:
          org.tron.protos.contract.SmartContractOuterClass.TriggerSmartContract triggerContract =
              contract.getParameter().unpack(org.tron.protos.contract.SmartContractOuterClass.TriggerSmartContract.class);
          return triggerContract.getCallValue();
        default:
          return 0;
      }
    } catch (Exception e) {
      return 0;
    }
  }

  /**
   * Extract transfer information from contract
   */
  private static void extractTransferInfo(TransactionLogTrigger trigger, Transaction.Contract contract) {
    try {
      switch (contract.getType()) {
        case TransferContract:
          TransferContract transferContract = contract.getParameter().unpack(TransferContract.class);
          trigger.setFromAddress(StringUtil.encode58Check(transferContract.getOwnerAddress().toByteArray()));
          trigger.setToAddress(StringUtil.encode58Check(transferContract.getToAddress().toByteArray()));
          trigger.setAssetAmount(transferContract.getAmount());
          trigger.setAssetName("trx");
          break;
        case TransferAssetContract:
          org.tron.protos.contract.AssetIssueContractOuterClass.TransferAssetContract transferAssetContract =
              contract.getParameter().unpack(org.tron.protos.contract.AssetIssueContractOuterClass.TransferAssetContract.class);
          trigger.setFromAddress(StringUtil.encode58Check(transferAssetContract.getOwnerAddress().toByteArray()));
          trigger.setToAddress(StringUtil.encode58Check(transferAssetContract.getToAddress().toByteArray()));
          trigger.setAssetAmount(transferAssetContract.getAmount());

          // Get TRC10 token ID - the asset_name field contains token ID when ALLOW_SAME_TOKEN_NAME is active
          // For TRC10 tokens, we want the token ID, not the name
          String tokenId = transferAssetContract.getAssetName().toStringUtf8();
          trigger.setAssetName(tokenId);
          break;
        case TriggerSmartContract:
          org.tron.protos.contract.SmartContractOuterClass.TriggerSmartContract triggerContract =
              contract.getParameter().unpack(org.tron.protos.contract.SmartContractOuterClass.TriggerSmartContract.class);
          trigger.setFromAddress(StringUtil.encode58Check(triggerContract.getOwnerAddress().toByteArray()));
          trigger.setToAddress(StringUtil.encode58Check(triggerContract.getContractAddress().toByteArray()));
          break;
        default:
          // For other contract types, try to extract owner address if available
          break;
      }
    } catch (Exception e) {
      // Ignore extraction errors
    }
  }

  /**
   * Process transactions in a block concurrently with batch TransactionInfo prefetching
   * PERFORMANCE OPTIMIZATION: Prefetch all TransactionInfo for a block in one sequential read
   */
  private static void processTransactionsConcurrently(List<TransactionCapsule> transactions,
      String blockId, long blockNum, long timestamp, String outputFormat,
      boolean useKafka, String kafkaTopic, Wallet wallet, ChainBaseManager chainBaseManager) {

    if (transactions.isEmpty()) {
      return;
    }

    logger.debug("Processing {} transactions concurrently...", transactions.size());

    // PERFORMANCE OPTIMIZATION: Batch prefetch all TransactionInfo for this block
    // This eliminates random I/O by reading all transaction info in one sequential operation
    Map<String, TransactionInfo> transactionInfoMap = new HashMap<>();
    try {
      if (chainBaseManager != null && chainBaseManager.getTransactionRetStore() != null) {
        // Get all TransactionInfo for this block in one sequential read
        byte[] blockNumKey = ByteArray.fromLong(blockNum);
        org.tron.core.capsule.TransactionRetCapsule transactionRetCapsule =
            chainBaseManager.getTransactionRetStore().getTransactionInfoByBlockNum(blockNumKey);

        if (transactionRetCapsule != null && transactionRetCapsule.getInstance() != null) {
          // Map all transaction IDs to their TransactionInfo
          for (TransactionInfo info : transactionRetCapsule.getInstance().getTransactioninfoList()) {
            String txId = ByteArray.toHexString(info.getId().toByteArray());
            transactionInfoMap.put(txId, info);
          }
          logger.debug("Batch prefetched {} TransactionInfo for block {}", transactionInfoMap.size(), blockNum);
        } else {
          logger.debug("No TransactionRetCapsule found for block {}, falling back to individual queries", blockNum);
        }
      }
    } catch (Exception e) {
      logger.warn("Failed to batch prefetch TransactionInfo for block {}: {}, falling back to individual queries",
                  blockNum, e.getMessage());
    }

    // Create a list to hold all the CompletableFuture tasks
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    AtomicInteger processedCount = new AtomicInteger(0);

    // Process each transaction concurrently
    for (int i = 0; i < transactions.size(); i++) {
      final int transactionIndex = i;
      final TransactionCapsule trx = transactions.get(i);
      final String txId = trx.getTransactionId().toString();

      // Get prefetched TransactionInfo from map (zero I/O!)
      final TransactionInfo prefetchedTransactionInfo = transactionInfoMap.get(txId);

      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        try {
          // Use prefetched TransactionInfo to avoid random I/O
          processTransactionWithPrefetchedInfo(trx, prefetchedTransactionInfo, transactionIndex,
                                               blockId, blockNum, timestamp,
                                               outputFormat, useKafka, kafkaTopic, wallet);

          int completed = processedCount.incrementAndGet();
          if (completed % 50 == 0 || completed == transactions.size()) {
            logger.debug("Processed {}/{} transactions in block {}", completed, transactions.size(), blockNum);
          }
        } catch (Exception e) {
          String errorMessage = "Error processing transaction " + (transactionIndex + 1) +
                               " in block " + blockNum + ": " + e.getMessage();
          logger.error(errorMessage, e);
        }
      }, transactionExecutor);

      futures.add(future);
    }

    // Wait for all transactions to complete
    try {
      CompletableFuture<Void> allFutures = CompletableFuture.allOf(
          futures.toArray(new CompletableFuture[0]));
      allFutures.get(); // This will block until all transactions are processed

      logger.debug("All {} transactions processed successfully in block {}", transactions.size(), blockNum);
    } catch (Exception e) {
      logger.error("Error waiting for concurrent transaction processing to complete: " + e.getMessage(), e);
    }
  }

  /**
   * Process a single transaction with prefetched TransactionInfo (OPTIMIZED)
   * This method uses prefetched TransactionInfo to eliminate random I/O
   */
  private static void processTransactionWithPrefetchedInfo(TransactionCapsule trx,
      TransactionInfo prefetchedTransactionInfo, int transactionIndex,
      String blockId, long blockNum, long timestamp, String outputFormat,
      boolean useKafka, String kafkaTopic, Wallet wallet) {

    String txId = trx.getTransactionId().toString();
    logger.debug("Processing transaction #{} (ID: {}) in block {}", transactionIndex + 1, txId, blockNum);

    // Use prefetched TransactionInfo (zero I/O!)
    TransactionInfo transactionInfo = prefetchedTransactionInfo;

    // If prefetch failed, fallback to individual query
    if (transactionInfo == null) {
      ByteString txIdBytes = ByteString.copyFrom(ByteArray.fromHexString(txId));
      transactionInfo = wallet.getTransactionInfoById(txIdBytes);
      logger.debug("Using fallback query for TransactionInfo (prefetch missed): {}", txId);
    }

    // Get transaction directly from TransactionCapsule (zero I/O!)
    Transaction transaction = null;
    try {
      transaction = trx.getInstance();
      logger.debug("Retrieved transaction directly from TransactionCapsule for ID: {}", txId);
    } catch (Exception e) {
      logger.warn("Could not get transaction from TransactionCapsule for ID {}: {}", txId, e.getMessage());
      // Fallback to database query only if TransactionCapsule fails
      ByteString txIdBytes = ByteString.copyFrom(ByteArray.fromHexString(txId));
      transaction = wallet.getTransactionById(txIdBytes);
    }

    // Process the transaction with the retrieved data
    processTransactionData(transactionInfo, transaction, trx, transactionIndex,
                          blockId, blockNum, timestamp, outputFormat, useKafka, kafkaTopic);
  }

  /**
   * Process a single transaction (used by concurrent processing - LEGACY METHOD)
   * This method is kept for backward compatibility but uses the new optimized path
   */
  private static void processTransaction(TransactionCapsule trx, int transactionIndex,
      String blockId, long blockNum, long timestamp, String outputFormat,
      boolean useKafka, String kafkaTopic, Wallet wallet) {

    // Use the optimized method with null prefetched info (will trigger fallback query)
    processTransactionWithPrefetchedInfo(trx, null, transactionIndex, blockId, blockNum,
                                        timestamp, outputFormat, useKafka, kafkaTopic, wallet);
  }

  /**
   * Process transaction data (shared logic for both optimized and legacy paths)
   */
  private static void processTransactionData(TransactionInfo transactionInfo, Transaction transaction,
      TransactionCapsule trx, int transactionIndex, String blockId, long blockNum, long timestamp,
      String outputFormat, boolean useKafka, String kafkaTopic) {

    String txId = trx.getTransactionId().toString();

    // Handle different output formats
    if ("trigger".equals(outputFormat)) {
      // Use TransactionLogTrigger format with optional Kafka sending
      String kafkaTopicToUse = useKafka ? kafkaTopic : null;
      String kafkaKey = txId; // Use transaction ID as Kafka key

      // Log if transaction data is missing
      if (transactionInfo == null && transaction == null) {
        logger.warn("Missing transaction data for ID {} in block {} - this may be normal for genesis block transactions", txId, blockNum);
      }

      printTransactionLogTrigger(transactionInfo, transaction, blockId, blockNum, timestamp, transactionIndex,
          "Transaction #" + (transactionIndex + 1), kafkaTopicToUse, kafkaKey, trx);
    } else {
      // Use traditional formats - synchronize output to prevent interleaving
      synchronized (System.out) {
        if (transactionInfo != null) {
          // Convert log addresses to TRON addresses while preserving internal transactions
          List<Log> newLogList = Util.convertLogAddressToTronAddress(transactionInfo);
          TransactionInfo transactionInfoWithConvertedLogs = transactionInfo.toBuilder()
              .clearLog()
              .addAllLog(newLogList)
              .build();

          if ("json".equals(outputFormat) || "both".equals(outputFormat)) {
            System.out.println(JsonFormat.printToString(transactionInfoWithConvertedLogs, true));
          }

          if ("protobuf".equals(outputFormat) || "both".equals(outputFormat)) {
            System.out.println(transactionInfoWithConvertedLogs.toString());
          }
        }

        if (transaction != null) {
          if ("json".equals(outputFormat) || "both".equals(outputFormat)) {
            System.out.println(JsonFormat.printToString(transaction, true));
          }

          if ("protobuf".equals(outputFormat) || "both".equals(outputFormat)) {
            System.out.println(transaction.toString());
          }
        }
      }
    }
  }

  /**
   * Main method to run the block transaction printer.
   * 
   * This method initializes the TRON environment, connects to the blockchain,
   * retrieves blocks in the specified range, and prints detailed information
   * about all transactions found in those blocks.
   *
   * The method processes blocks in batches to avoid memory issues when dealing
   * with large block ranges. For each block, it prints:
   * - Block number, ID, and timestamp
   * - Number of transactions in the block
   * - For each transaction:
   *   - Transaction ID and status
   *   - Contract type
   *   - From/To addresses
   *   - Amount (for transfer contracts)
   *   - Call value (for smart contract calls)
   *   - Timestamp and expiration
   *
   * After processing all blocks, it prints a summary of the total blocks and
   * transactions processed.
   *
   * @param args Command line arguments:
   *             args[0] - Start block number (inclusive)
   *             args[1] - End block number (inclusive)
   *             args[2+] - Optional configuration parameters:
   *                       -c <config_file>: Specify a custom configuration file
   *                       -d <data_dir>: Specify a custom data directory
   *                       (Other standard TRON node options are also supported)
   */
  public static void main(String[] args) {
    logger.info("BlockTransactionPrinter started");

    if (args.length < 2) {
      System.out.println("Usage:");
      System.out.println("  BlockTransactionPrinter <startBlockNum> <endBlockNum> [options]");
      System.out.println("  BlockTransactionPrinter -tx <transactionId> [options]");
      System.out.println("Options:");
      System.out.println("  -c <config_file>: Specify a custom configuration file");
      System.out.println("  -d <data_dir>: Specify a custom data directory");
      System.out.println("  -fm <format>: Output format (json|protobuf|both|trigger), default: both");
      System.out.println("  -kb <brokers>: Kafka broker addresses (e.g., localhost:9092,broker2:9092)");
      System.out.println("  -kt <topic>: Kafka topic name for sending trigger data (requires -kb)");
      System.out.println("  -threads <count>: Number of threads for concurrent transaction processing, default: " + DEFAULT_THREAD_POOL_SIZE);
      return;
    }

    // Parse output format option
    String outputFormat = "both"; // default
    for (int i = 0; i < args.length - 1; i++) {
      if ("-fm".equals(args[i])) {
        outputFormat = args[i + 1].toLowerCase();
        logger.info("Output format set to: {}", outputFormat);
        break;
      }
    }

    // Parse Kafka options and thread pool size
    String kafkaBrokers = null;
    String kafkaTopic = null;
    int customThreadPoolSize = DEFAULT_THREAD_POOL_SIZE;

    for (int i = 0; i < args.length - 1; i++) {
      if ("-kb".equals(args[i])) {
        kafkaBrokers = args[i + 1];
        logger.info("Kafka brokers set to: {}", kafkaBrokers);
      } else if ("-kt".equals(args[i])) {
        kafkaTopic = args[i + 1];
        logger.info("Kafka topic set to: {}", kafkaTopic);
      } else if ("-threads".equals(args[i])) {
        try {
          customThreadPoolSize = Integer.parseInt(args[i + 1]);
          if (customThreadPoolSize <= 0) {
            logger.warn("Thread pool size must be positive. Using default: {}", DEFAULT_THREAD_POOL_SIZE);
            customThreadPoolSize = DEFAULT_THREAD_POOL_SIZE;
          } else {
            logger.info("Thread pool size set to: {}", customThreadPoolSize);
          }
        } catch (NumberFormatException e) {
          logger.warn("Invalid thread pool size. Using default: {}", DEFAULT_THREAD_POOL_SIZE);
          customThreadPoolSize = DEFAULT_THREAD_POOL_SIZE;
        }
      }
    }

    // Validate Kafka configuration
    boolean useKafka = kafkaBrokers != null && kafkaTopic != null;
    if (kafkaBrokers != null && kafkaTopic == null) {
      logger.error("Kafka brokers specified but no topic provided. Use -kt to specify topic.");
      return;
    }
    if (kafkaBrokers == null && kafkaTopic != null) {
      logger.error("Kafka topic specified but no brokers provided. Use -kb to specify brokers.");
      return;
    }
    if (useKafka && !"trigger".equals(outputFormat)) {
      logger.warn("Kafka output is only supported with 'trigger' format. Current format: {}. Kafka output will be disabled.", outputFormat);
      useKafka = false;
    }

    // Validate output format
    if (!outputFormat.equals("json") && !outputFormat.equals("protobuf") &&
        !outputFormat.equals("both") && !outputFormat.equals("trigger")) {
      logger.error("Invalid output format: {}. Use 'json', 'protobuf', 'both', or 'trigger'", outputFormat);
      return;
    }

    // Check if this is transaction ID mode
    boolean isTransactionMode = "-tx".equals(args[0]);
    String transactionId = null;
    long startBlockNum = 0;
    long endBlockNum = 0;

    try {
      if (isTransactionMode) {
        if (args.length < 2) {
          logger.error("Transaction ID is required when using -tx option");
          return;
        }
        transactionId = args[1];
        // Validate transaction ID format (should be 64 character hex string)
        if (transactionId.length() != 64 || !transactionId.matches("[0-9a-fA-F]+")) {
          logger.error("Invalid transaction ID format. Expected 64-character hex string.");
          return;
        }
      } else {
        startBlockNum = Long.parseLong(args[0]);
        endBlockNum = Long.parseLong(args[1]);

        if (startBlockNum < 0 || endBlockNum < 0 || startBlockNum > endBlockNum) {
          logger.error("Invalid block range. Start block must be <= end block and both must be >= 0");
          return;
        }
      }

      // Initialize statistics tracking
      initializeStatistics(startBlockNum);

      // Initialize thread pool for concurrent transaction processing
      initializeThreadPool(customThreadPoolSize);

      // Initialize Kafka if configured
      if (useKafka) {
        initKafkaProducer(kafkaBrokers);
        if (kafkaProducer == null) {
          logger.warn("Failed to initialize Kafka producer. Continuing without Kafka.");
          useKafka = false;
        }
      }

      // Initialize TRON environment
      // Create a new array without the block numbers/transaction ID and custom parameters for Args.setParam
      String[] configArgs;
      int configStartIndex = isTransactionMode ? 2 : 2; // Both modes skip first 2 args

      // Filter out custom parameters (-fm, -kb, -kt, -threads) and their values since they're not TRON node parameters
      List<String> filteredArgs = new ArrayList<>();
      for (int i = configStartIndex; i < args.length; i++) {
        if ("-fm".equals(args[i]) || "-kb".equals(args[i]) || "-kt".equals(args[i]) || "-threads".equals(args[i])) {
          // Skip custom parameter and its value
          i++; // Skip the next argument (parameter value)
        } else {
          filteredArgs.add(args[i]);
        }
      }

      configArgs = filteredArgs.toArray(new String[0]);

      // Disable asset update to avoid "Asset num is wrong!" error and database clearing
      CommonParameter.getInstance().setNeedToUpdateAsset(false);

      // Disable unnecessary services for database-only operation
      CommonParameter.getInstance().setP2pDisable(true);
      CommonParameter.getInstance().setRpcEnable(false);
      CommonParameter.getInstance().setRpcSolidityEnable(false);
      CommonParameter.getInstance().setRpcPBFTEnable(false);
      CommonParameter.getInstance().setFullNodeHttpEnable(false);
      CommonParameter.getInstance().setSolidityNodeHttpEnable(false);
      CommonParameter.getInstance().setPBFTHttpEnable(false);
      CommonParameter.getInstance().setJsonRpcHttpFullNodeEnable(false);
      CommonParameter.getInstance().setJsonRpcHttpSolidityNodeEnable(false);
      CommonParameter.getInstance().setJsonRpcHttpPBFTNodeEnable(false);
      CommonParameter.getInstance().setEventSubscribe(false);
      CommonParameter.getInstance().setNodeMetricsEnable(false);
      CommonParameter.getInstance().setMetricsStorageEnable(false);
      CommonParameter.getInstance().setMetricsPrometheusEnable(false);

      // Enable read-only mode for database operations
      logger.info("Enabling database read-only mode");
      System.setProperty("database.readonly", "true");
      System.setProperty("storage.readonly", "true");

      Args.setParam(configArgs, Constant.TESTNET_CONF);

      // Configure storage for read-only mode
      if (Args.getInstance().getStorage() != null) {
        Args.getInstance().getStorage().setDbSync(false);
        Args.getInstance().getStorage().setMaxFlushCount(0); // Disable flushing to prevent writes
        logger.debug("Database sync disabled for read-only mode");
      }

      // Log database path
      String databasePath = Args.getInstance().getOutputDirectory();
      logger.info("Database directory: {}", databasePath);

      File dbDir = new File(databasePath);
      if (!dbDir.exists()) {
        logger.error("Database directory does not exist: {}", databasePath);
        return;
      }

      logger.info("Initializing database-only configuration");
      DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
      beanFactory.setAllowCircularReferences(false);
      TronApplicationContext context = new TronApplicationContext(beanFactory);
      context.register(DefaultConfig.class);
      context.refresh();
      logger.info("Database context initialized successfully");

      // Check internal transaction configuration
      boolean saveInternalTx = CommonParameter.getInstance().isSaveInternalTx();
      boolean saveFeaturedInternalTx = CommonParameter.getInstance().isSaveFeaturedInternalTx();
      logger.info("Internal transaction configuration - Save: {}, SaveFeatured: {}", saveInternalTx, saveFeaturedInternalTx);
      if (!saveInternalTx) {
        logger.warn("Internal transactions are not being saved! Set 'vm.saveInternalTx = true' in config.conf to enable.");
      }

      // Get ChainBaseManager and Wallet instances
      ChainBaseManager chainBaseManager = context.getBean(ChainBaseManager.class);
      Wallet wallet = context.getBean(Wallet.class);

      logger.debug("ChainBaseManager initialized: {}", (chainBaseManager != null));
      logger.debug("Wallet initialized: {}", (wallet != null));

      // Check if database stores are accessible
      try {
        boolean blockStoreEmpty = chainBaseManager.getBlockStore().isNotEmpty();
        logger.debug("BlockStore is not empty: {}", blockStoreEmpty);
      } catch (Exception e) {
        logger.warn("Error checking BlockStore: {}", e.getMessage());
      }

      try {
        boolean dynamicPropsStoreAccessible = chainBaseManager.getDynamicPropertiesStore() != null;
        logger.debug("DynamicPropertiesStore accessible: {}", dynamicPropsStoreAccessible);
      } catch (Exception e) {
        logger.warn("Error accessing DynamicPropertiesStore: {}", e.getMessage());
      }

      // Get database block range using multiple approaches for robustness
      long lowestBlockNum = 0;
      long latestBlockNum = 0;

      // Try to get the lowest block number
      try {
        lowestBlockNum = chainBaseManager.getLowestBlockNum();

        // Check if this is a lite node (doesn't have genesis block)
        boolean isLiteNode = chainBaseManager.isLiteNode();
        logger.info("Node type: {}", (isLiteNode ? "Lite Node" : "Full Node"));

        if (lowestBlockNum < 0) {
          // If getLowestBlockNum returns -1 or negative, try alternative approach
          List<BlockCapsule> firstBlocks = chainBaseManager.getBlockStore().getLimitNumber(0, 1);
          if (!firstBlocks.isEmpty()) {
            lowestBlockNum = firstBlocks.get(0).getNum();
          } else {
            lowestBlockNum = 0;
          }
        }

        // For full nodes, check if genesis block (block 0) is accessible
        if (!isLiteNode) {
          try {
            BlockCapsule genesisBlock = chainBaseManager.getGenesisBlock();
            if (genesisBlock != null) {
              logger.debug("Genesis block found: {}", genesisBlock.getBlockId().toString());
              // For full nodes, lowest block should be 0 if genesis block exists
              lowestBlockNum = 0;
            }
          } catch (Exception genesisException) {
            logger.warn("Could not access genesis block: {}", genesisException.getMessage());
          }
        } else {
          logger.info("Lite node detected - lowest available block: {}", lowestBlockNum);
        }

      } catch (Exception e) {
        logger.warn("Could not get lowest block number, using 0. Error: {}", e.getMessage());
        lowestBlockNum = 0;
      }

      // Try to get the latest block number
      try {
        latestBlockNum = chainBaseManager.getDynamicPropertiesStore().getLatestBlockHeaderNumber();
      } catch (Exception e) {
        System.out.println("Warning: Could not get latest block number from DynamicPropertiesStore, trying alternative approach. Error: " + e.getMessage());
        try {
          // Try alternative approach using getLatestBlockHeaderNumberFromDB
          latestBlockNum = chainBaseManager.getDynamicPropertiesStore().getLatestBlockHeaderNumberFromDB();
          if (latestBlockNum < 0) {
            // If that also fails, try getting the latest blocks from BlockStore
            List<BlockCapsule> latestBlocks = chainBaseManager.getBlockStore().getBlockByLatestNum(1);
            if (!latestBlocks.isEmpty()) {
              latestBlockNum = latestBlocks.get(0).getNum();
            } else {
              latestBlockNum = 0;
            }
          }
        } catch (Exception e2) {
          System.out.println("Warning: Could not get latest block number from any source, using 0. Error: " + e2.getMessage());
          latestBlockNum = 0;
        }
      }

      String blockRangeMessage = "Database block range: " + lowestBlockNum + " to " + latestBlockNum;
      System.out.println("=== " + blockRangeMessage + " ===");
      logger.info(blockRangeMessage);

      // Check if we have a valid block range
      if (latestBlockNum == 0 && lowestBlockNum == 0) {
        System.out.println("Warning: Database appears to be empty or not properly initialized.");
        System.out.println("This could happen if:");
        System.out.println("1. The database directory is empty or doesn't exist");
        System.out.println("2. The node hasn't synchronized any blocks yet");
        System.out.println("3. The configuration is pointing to the wrong database directory");
        System.out.println("Please check your configuration and ensure the database contains blockchain data.");
        context.close();
        System.out.println("=== Read-Only Database Query Failed - Exiting Program ===");
        return;
      }

      // Handle transaction mode
      if (isTransactionMode) {
        String queryMessage = "Querying transaction: " + transactionId;
        System.out.println("=== " + queryMessage + " ===");
        logger.info(queryMessage);

        try {
          ByteString txIdBytes = ByteString.copyFrom(ByteArray.fromHexString(transactionId));
          TransactionInfo transactionInfo = wallet.getTransactionInfoById(txIdBytes);
          Transaction transaction = wallet.getTransactionById(txIdBytes);

          // Check if transaction was found before processing
          if (transactionInfo == null && transaction == null) {
            String notFoundMessage = "Transaction not found: " + transactionId;
            System.out.println(notFoundMessage);
            logger.warn(notFoundMessage);
            System.out.println("This could happen if:");
            System.out.println("1. The transaction ID is incorrect");
            System.out.println("2. The transaction is not in this database");
            System.out.println("3. The transaction is too old and has been pruned");
            // Don't process or send to Kafka if transaction is not found
            return;
          }

          if ("trigger".equals(outputFormat)) {
            // For trigger format, we need block information
            String blockHash = "N/A";
            long blockNumber = 0;
            long timestamp = 0;
            int transactionIndex = 0;

            if (transactionInfo != null) {
              blockNumber = transactionInfo.getBlockNumber();
              timestamp = transactionInfo.getBlockTimeStamp();

              // Try to get block hash from block number
              try {
                BlockCapsule blockCapsule = chainBaseManager.getBlockByNum(blockNumber);
                if (blockCapsule != null) {
                  blockHash = blockCapsule.getBlockId().toString();
                }
              } catch (Exception e) {
                System.out.println("Warning: Could not retrieve block hash for block " + blockNumber);
                logger.warn("Could not retrieve block hash for block {}: {}", blockNumber, e.getMessage());
              }
            }

            String kafkaTopicToUse = useKafka ? kafkaTopic : null;
            String kafkaKey = transactionId; // Use transaction ID as Kafka key
            printTransactionLogTrigger(transactionInfo, transaction, blockHash, blockNumber, timestamp, transactionIndex,
                "Transaction (TransactionLogTrigger Format)", kafkaTopicToUse, kafkaKey);
          } else {
            // Use traditional formats
            printTransactionInfo(transactionInfo, outputFormat, "Transaction Info (wallet/gettransactioninfobyid)");
            printTransaction(transaction, outputFormat, "Transaction Details (walletsolidity/gettransactionbyid)");
          }
        } catch (Exception e) {
          System.out.println("Error querying transaction: " + e.getMessage());
          e.printStackTrace();
        }

        // Shutdown and exit for transaction mode
        context.close();
        System.out.println("=== Read-Only Database Query Completed - Exiting Program ===");
        return;
      }

      // Validate user input against database range (block range mode)
      if (startBlockNum < lowestBlockNum || endBlockNum > latestBlockNum) {
        String errorMessage = "Error: Requested block range (" + startBlockNum + " to " + endBlockNum +
                          ") is outside the available range (" + lowestBlockNum + " to " + latestBlockNum + ")";
        System.out.println(errorMessage);
        logger.error(errorMessage);

        // Special message for genesis block queries
        if (startBlockNum == 0 && lowestBlockNum > 0) {
          System.out.println("\n=== Genesis Block (Block 0) Information ===");
          System.out.println("You are trying to query genesis block (block 0), but this node's lowest available block is " + lowestBlockNum);
          System.out.println("This could happen because:");
          System.out.println("1. This is a Lite Node that doesn't store early blocks");
          System.out.println("2. The database has been pruned to save space");
          System.out.println("3. This is a snapshot database that starts from a later block");
          System.out.println("\nTo query genesis block, you need:");
          System.out.println("- A Full Node with complete blockchain data");
          System.out.println("- Or access to a blockchain explorer");
          System.out.println("- Or a database that includes the genesis block");

          // Try to get genesis block information anyway
          try {
            BlockCapsule genesisBlock = chainBaseManager.getGenesisBlock();
            if (genesisBlock != null) {
              System.out.println("\nGenesis block information (from memory):");
              System.out.println("Block ID: " + genesisBlock.getBlockId().toString());
              System.out.println("Block Number: " + genesisBlock.getNum());
              System.out.println("Timestamp: " + genesisBlock.getTimeStamp());
              System.out.println("Transactions: " + genesisBlock.getTransactions().size());
              logger.info("Genesis block found in memory: {}", genesisBlock.getBlockId().toString());
            }
          } catch (Exception e) {
            System.out.println("Could not retrieve genesis block information: " + e.getMessage());
            logger.warn("Could not retrieve genesis block information: {}", e.getMessage());
          }
        }

        context.close();
        System.out.println("=== Read-Only Database Query Failed - Exiting Program ===");
        return;
      }

      String processingMessage = "Printing transactions from block " + startBlockNum + " to " + endBlockNum;
      System.out.println("=== " + processingMessage + " ===");
      logger.info(processingMessage);

      // Initialize counters for summary
      int totalBlocks = 0;
      int totalTransactions = 0;

      // Date formatter for timestamps
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

      // Process blocks in batches to avoid memory issues
      long batchSize = 1000;
      for (long currentStart = startBlockNum; currentStart <= endBlockNum; currentStart += batchSize) {
        long currentEnd = Math.min(currentStart + batchSize - 1, endBlockNum);
        long limit = currentEnd - currentStart + 1;

        logger.debug("Fetching blocks from {} to {}", currentStart, currentEnd);

        // Get blocks in the current batch
        List<BlockCapsule> blocks;

        // Special handling for genesis block (block 0)
        if (currentStart == 0) {
          blocks = new ArrayList<>();
          try {
            // Try to get genesis block directly
            BlockCapsule genesisBlock = chainBaseManager.getGenesisBlock();
            if (genesisBlock != null && genesisBlock.getNum() == 0) {
              blocks.add(genesisBlock);
              logger.debug("Successfully retrieved genesis block directly");
            }
          } catch (Exception e) {
            logger.warn("Could not get genesis block directly: {}", e.getMessage());
          }

          // If we need more blocks after genesis block, get them from BlockStore
          if (limit > 1) {
            try {
              List<BlockCapsule> additionalBlocks = chainBaseManager.getBlockStore().getLimitNumber(1, limit - 1);
              blocks.addAll(additionalBlocks);
            } catch (Exception e) {
              logger.warn("Could not get blocks after genesis: {}", e.getMessage());
            }
          }

          // If we still don't have any blocks, try the normal approach
          if (blocks.isEmpty()) {
            try {
              blocks = chainBaseManager.getBlockStore().getLimitNumber(currentStart, limit);
            } catch (Exception e) {
              logger.warn("Could not get blocks using normal approach: {}", e.getMessage());
              blocks = new ArrayList<>();
            }
          }
        } else {
          // Normal block retrieval for non-genesis blocks
          blocks = chainBaseManager.getBlockStore().getLimitNumber(currentStart, limit);
        }

        totalBlocks += blocks.size();

        // Track batch processing for statistics
        int batchTransactions = 0;
        long lastBlockInBatch = currentStart; // Default to current start

        // Process each block
        for (BlockCapsule block : blocks) {
          long blockNum = block.getNum();
          String blockId = block.getBlockId().toString();
          long timestamp = block.getTimeStamp();
          lastBlockInBatch = blockNum; // Track the last block number in this batch

          // Process transactions in the block
          List<TransactionCapsule> transactions = block.getTransactions();
          totalTransactions += transactions.size();
          batchTransactions += transactions.size();

          // Log block processing details
          logger.debug("Processing Block #{} with {} transactions", blockNum, transactions.size());

          // Use concurrent processing for transactions within the block
          processTransactionsConcurrently(transactions, blockId, blockNum, timestamp,
                                         outputFormat, useKafka, kafkaTopic, wallet, chainBaseManager);
        }

        // Update statistics after processing each batch
        updateStatistics(blocks.size(), batchTransactions, lastBlockInBatch);
      }

      // Log and print final statistics
      logFinalStatistics();

      // Log summary to file
      double avgTxPerBlock = totalBlocks > 0 ? (double)totalTransactions / totalBlocks : 0;
      String summaryMessage = String.format("Processing completed - Total blocks: %d, Total transactions: %d, Average tx/block: %.2f",
                                          totalBlocks, totalTransactions, avgTxPerBlock);
      logger.info(summaryMessage);

      // Shutdown the context
      context.close();
      logger.info("Transaction printing completed successfully");

    } catch (NumberFormatException e) {
      logger.error("Block numbers must be valid integers", e);
    } catch (Exception e) {
      logger.error("Error: " + e.getMessage(), e);
    } finally {
      // Shutdown thread pool gracefully
      shutdownThreadPool();

      // Close Kafka producer if it was initialized
      closeKafkaProducer();
    }
  }
}
