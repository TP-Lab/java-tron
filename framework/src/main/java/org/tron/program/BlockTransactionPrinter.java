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
import org.tron.common.parameter.CommonParameter;
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
 *    java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -f trigger
 *    java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -f json
 *    java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -f trigger -kb localhost:9092 -kt tron-transactions
 *    java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter -tx <txid> -f trigger -kb localhost:9092 -kt tron-transactions
 * 
 * Options:
 *    -c <config_file>: Specify a custom configuration file
 *    -d <data_dir>: Specify a custom data directory
 *    -f <format>: Output format, default: both
 *        json     - Standard JSON format using JsonFormat
 *        protobuf - Protobuf toString format
 *        both     - Both JSON and protobuf formats
 *        trigger  - TransactionLogTrigger format (structured JSON with comprehensive transaction data)
 *    -kb <kafka_brokers>: Kafka broker addresses (e.g., localhost:9092,broker2:9092)
 *    -kt <kafka_topic>: Kafka topic name for sending trigger data
 *    (Other standard TRON node options are also supported)
 * 
 * The program will:
 * - Connect to the TRON blockchain using the specified configuration
 * - Retrieve blocks in the specified range (from startBlockNum to endBlockNum)
 * - Print detailed information about each block and its transactions
 * - Show a summary of the total blocks and transactions processed
 * 
 * Note: Make sure you have a running TRON node or a valid database directory
 * configured to access the blockchain data.
 */
@Slf4j(topic = "app")
public class BlockTransactionPrinter {

  private static KafkaProducer<String, String> kafkaProducer = null;

  // Statistics tracking
  private static long startTime = 0;
  private static long totalBlocksProcessed = 0;
  private static long totalTransactionsProcessed = 0;
  private static long lastStatsTime = 0;
  private static long lastBlocksProcessed = 0;
  private static long lastTransactionsProcessed = 0;

  /**
   * Initialize statistics tracking
   */
  private static void initializeStatistics() {
    startTime = System.currentTimeMillis();
    lastStatsTime = startTime;
    totalBlocksProcessed = 0;
    totalTransactionsProcessed = 0;
    lastBlocksProcessed = 0;
    lastTransactionsProcessed = 0;
    logger.info("Statistics tracking initialized");
  }

  /**
   * Update and log statistics
   */
  private static void updateStatistics(int blocksProcessed, int transactionsProcessed) {
    totalBlocksProcessed += blocksProcessed;
    totalTransactionsProcessed += transactionsProcessed;

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
        "Processing Statistics - Total: %d blocks, %d transactions in %.1fs " +
        "(%.2f blocks/s, %.2f tx/s) | Recent: %d blocks, %d transactions in %.1fs " +
        "(%.2f blocks/s, %.2f tx/s)",
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
      "Final Statistics - Processed %d blocks and %d transactions in %.1f seconds " +
      "(Average: %.2f blocks/s, %.2f transactions/s)",
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

      kafkaProducer = new KafkaProducer<>(props);
      String kafkaInitMessage = "Kafka producer initialized successfully with brokers: " + kafkaBrokers;
      System.out.println(kafkaInitMessage);
      logger.info(kafkaInitMessage);
    } catch (Exception e) {
      String kafkaErrorMessage = "Failed to initialize Kafka producer: " + e.getMessage();
      System.err.println(kafkaErrorMessage);
      logger.error(kafkaErrorMessage, e);
      e.printStackTrace();
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
          System.err.println(errorMessage);
          logger.error(errorMessage, exception);
          exception.printStackTrace();
        } else {
          String successMessage = "Message sent to Kafka topic '" + topic + "' at offset " + metadata.offset();
          System.out.println(successMessage);
          logger.debug(successMessage);
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
        String closeMessage = "Kafka producer closed successfully.";
        System.out.println(closeMessage);
        logger.info(closeMessage);
      } catch (Exception e) {
        String errorMessage = "Error closing Kafka producer: " + e.getMessage();
        System.err.println(errorMessage);
        logger.error(errorMessage, e);
        e.printStackTrace();
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

    System.out.println("=== " + title + " ===");

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
    printTransactionLogTrigger(transactionInfo, transaction, blockHash, blockNumber, timestamp, transactionIndex, title, null, null);
  }

  /**
   * Print transaction in TransactionLogTrigger format with optional Kafka sending
   */
  private static void printTransactionLogTrigger(TransactionInfo transactionInfo, Transaction transaction,
      String blockHash, long blockNumber, long timestamp, int transactionIndex, String title,
      String kafkaTopic, String kafkaKey) {

    System.out.println("=== " + title + " ===");

    // Check if we have valid transaction data before proceeding
    if (transactionInfo == null && transaction == null) {
      System.out.println("No transaction data available - skipping TransactionLogTrigger creation and Kafka sending");
      logger.warn("Attempted to create TransactionLogTrigger with null transactionInfo and transaction");
      return;
    }

    // For genesis block transactions, we might only have transaction data without transactionInfo
    if (transactionInfo == null && transaction != null) {
      System.out.println("Note: Creating TransactionLogTrigger from transaction data only (no TransactionInfo available)");
      logger.info("Creating TransactionLogTrigger from transaction data only for block {}", blockNumber);
    }

    try {
      TransactionLogTrigger trigger = createTransactionLogTrigger(
          transactionInfo, transaction, blockHash, blockNumber, timestamp, transactionIndex);

      System.out.println("--- TransactionLogTrigger Format ---");
      String jsonOutput = JsonUtil.obj2Json(trigger);
      if (jsonOutput != null) {
        System.out.println(jsonOutput);

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
            logger.debug("TransactionLogTrigger sent to Kafka topic: {} with key: {}", kafkaTopic, key);
          } else {
            System.out.println("Skipping Kafka send - invalid or missing transaction ID: " + trigger.getTransactionId());
            logger.warn("Skipped Kafka send due to invalid transaction ID: {}", trigger.getTransactionId());
          }
        }
      } else {
        System.out.println("Failed to serialize TransactionLogTrigger to JSON");
        logger.error("Failed to serialize TransactionLogTrigger to JSON for transaction");
      }
    } catch (Exception e) {
      String errorMessage = "Error creating TransactionLogTrigger: " + e.getMessage();
      System.out.println(errorMessage);
      logger.error(errorMessage, e);
      e.printStackTrace();
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

    System.out.println("=== " + title + " ===");

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

    TransactionLogTrigger trigger = new TransactionLogTrigger();

    // Basic transaction information
    if (transactionInfo != null) {
      trigger.setTransactionId(Hex.toHexString(transactionInfo.getId().toByteArray()));
    } else if (transaction != null) {
      // For genesis block transactions, calculate transaction ID from the transaction itself
      try {
        // Use the transaction's hash as the transaction ID
        byte[] txBytes = transaction.toByteArray();
        String calculatedTxId = Hex.toHexString(Sha256Hash.hash(true, txBytes));
        trigger.setTransactionId(calculatedTxId);
        System.out.println("    Calculated transaction ID from transaction data: " + calculatedTxId);
      } catch (Exception e) {
        trigger.setTransactionId("GENESIS_TX_" + transactionIndex);
        System.out.println("    Using fallback transaction ID: GENESIS_TX_" + transactionIndex);
      }
    } else {
      // Last resort: use a placeholder ID
      trigger.setTransactionId("UNKNOWN_TX_" + transactionIndex);
    }

    trigger.setBlockHash(blockHash);
    trigger.setBlockNumber(blockNumber);
    trigger.setTimeStamp(timestamp);
    trigger.setTransactionIndex(transactionIndex);

    // Transaction data
    if (transaction != null && transaction.getRawData() != null) {
      trigger.setData(Hex.toHexString(transaction.getRawData().getData().toByteArray()));
      trigger.setFeeLimit(transaction.getRawData().getFeeLimit());

      // Contract information
      if (transaction.getRawData().getContractCount() > 0) {
        Transaction.Contract contract = transaction.getRawData().getContract(0);
        trigger.setContractType(contract.getType().toString());
        trigger.setContractCallValue(getCallValue(contract));
        trigger.setContractData(contract.getParameter().toString());

        // Extract transfer information for transfer contracts
        extractTransferInfo(trigger, contract);
      }
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

      // ExtMap - use getCancelUnfreezeV2AmountMap() from TransactionInfo if available
      if (transactionInfo.getCancelUnfreezeV2AmountCount() > 0) {
        // TransactionInfo has getCancelUnfreezeV2AmountMap() which is exactly what extMap should contain
        trigger.setExtMap(transactionInfo.getCancelUnfreezeV2AmountMap());
      } else {
        // Initialize empty extMap for cases without cancel unfreeze data
        Map<String, Long> extMap = new HashMap<>();
        trigger.setExtMap(extMap);
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
          trigger.setAssetName(transferAssetContract.getAssetName().toStringUtf8());
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
    // Test logging immediately to verify configuration
    System.out.println("=== BlockTransactionPrinter Starting ===");
    logger.info("BlockTransactionPrinter started - testing log configuration");
    logger.debug("Debug level logging test");
    logger.warn("Warning level logging test");

    if (args.length < 2) {
      String usageMessage = "Insufficient arguments provided";
      System.out.println("Usage:");
      System.out.println("  BlockTransactionPrinter <startBlockNum> <endBlockNum> [options]");
      System.out.println("  BlockTransactionPrinter -tx <transactionId> [options]");
      System.out.println("Options:");
      System.out.println("  -c <config_file>: Specify a custom configuration file");
      System.out.println("  -d <data_dir>: Specify a custom data directory");
      System.out.println("  -f <format>: Output format (json|protobuf|both|trigger), default: both");
      System.out.println("  -kb <brokers>: Kafka broker addresses (e.g., localhost:9092,broker2:9092)");
      System.out.println("  -kt <topic>: Kafka topic name for sending trigger data (requires -kb)");
      logger.error(usageMessage);
      return;
    }

    // Parse output format option
    String outputFormat = "both"; // default
    for (int i = 0; i < args.length - 1; i++) {
      if ("-f".equals(args[i])) {
        outputFormat = args[i + 1].toLowerCase();
        System.out.println("Output format set to: " + outputFormat);
        break;
      }
    }

    // Parse Kafka options
    String kafkaBrokers = null;
    String kafkaTopic = null;
    for (int i = 0; i < args.length - 1; i++) {
      if ("-kb".equals(args[i])) {
        kafkaBrokers = args[i + 1];
        System.out.println("Kafka brokers set to: " + kafkaBrokers);
      } else if ("-kt".equals(args[i])) {
        kafkaTopic = args[i + 1];
        System.out.println("Kafka topic set to: " + kafkaTopic);
      }
    }

    // Validate Kafka configuration
    boolean useKafka = kafkaBrokers != null && kafkaTopic != null;
    if (kafkaBrokers != null && kafkaTopic == null) {
      System.out.println("Error: Kafka brokers specified but no topic provided. Use -kt to specify topic.");
      return;
    }
    if (kafkaBrokers == null && kafkaTopic != null) {
      System.out.println("Error: Kafka topic specified but no brokers provided. Use -kb to specify brokers.");
      return;
    }
    if (useKafka && !"trigger".equals(outputFormat)) {
      System.out.println("Warning: Kafka output is only supported with 'trigger' format. Current format: " + outputFormat);
      System.out.println("Kafka output will be disabled.");
      useKafka = false;
    }

    // Validate output format
    if (!outputFormat.equals("json") && !outputFormat.equals("protobuf") &&
        !outputFormat.equals("both") && !outputFormat.equals("trigger")) {
      System.out.println("Error: Invalid output format. Use 'json', 'protobuf', 'both', or 'trigger'");
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
          System.out.println("Error: Transaction ID is required when using -tx option");
          return;
        }
        transactionId = args[1];
        // Validate transaction ID format (should be 64 character hex string)
        if (transactionId.length() != 64 || !transactionId.matches("[0-9a-fA-F]+")) {
          System.out.println("Error: Invalid transaction ID format. Expected 64-character hex string.");
          return;
        }
      } else {
        startBlockNum = Long.parseLong(args[0]);
        endBlockNum = Long.parseLong(args[1]);

        if (startBlockNum < 0 || endBlockNum < 0 || startBlockNum > endBlockNum) {
          System.out.println("Invalid block range. Start block must be <= end block and both must be >= 0");
          return;
        }
      }

      // Initialize statistics tracking
      initializeStatistics();

      // Initialize Kafka if configured
      if (useKafka) {
        initKafkaProducer(kafkaBrokers);
        if (kafkaProducer == null) {
          String kafkaFailMessage = "Failed to initialize Kafka producer. Continuing without Kafka.";
          System.out.println(kafkaFailMessage);
          logger.warn(kafkaFailMessage);
          useKafka = false;
        }
      }

      // Initialize TRON environment
      // Create a new array without the block numbers/transaction ID and custom parameters for Args.setParam
      String[] configArgs;
      int configStartIndex = isTransactionMode ? 2 : 2; // Both modes skip first 2 args

      // Filter out custom parameters (-f, -kb, -kt) and their values since they're not TRON node parameters
      List<String> filteredArgs = new ArrayList<>();
      for (int i = configStartIndex; i < args.length; i++) {
        if ("-f".equals(args[i]) || "-kb".equals(args[i]) || "-kt".equals(args[i])) {
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
      System.out.println("=== Enabling Database Read-Only Mode ===");
      System.setProperty("database.readonly", "true");
      System.setProperty("storage.readonly", "true");

      Args.setParam(configArgs, Constant.TESTNET_CONF);

      // Configure storage for read-only mode
      if (Args.getInstance().getStorage() != null) {
        Args.getInstance().getStorage().setDbSync(false);
        Args.getInstance().getStorage().setMaxFlushCount(0); // Disable flushing to prevent writes
        System.out.println("Database sync disabled for read-only mode");
        System.out.println("Database flush count set to 0 for read-only mode");
      }

      // Print detailed database initialization information
      System.out.println("=== Database Initialization Information ===");
      String databasePath = Args.getInstance().getOutputDirectory();
      System.out.println("Database directory path: " + databasePath);

      File dbDir = new File(databasePath);
      System.out.println("Database directory exists: " + dbDir.exists());
      System.out.println("Database directory is directory: " + dbDir.isDirectory());
      System.out.println("Database directory absolute path: " + dbDir.getAbsolutePath());

      if (dbDir.exists()) {
        System.out.println("Database directory is readable: " + dbDir.canRead());
        System.out.println("Database directory is writable: " + dbDir.canWrite());

        File[] files = dbDir.listFiles();
        if (files != null) {
          System.out.println("Contents of database directory (" + files.length + " items):");
          for (File file : files) {
            String type = file.isDirectory() ? "[DIR]" : "[FILE]";
            long size = file.isFile() ? file.length() : 0;
            System.out.println("  " + type + " " + file.getName() + 
                             (file.isFile() ? " (" + size + " bytes)" : ""));
          }
        } else {
          System.out.println("Could not list contents of database directory (permission denied?)");
        }
      } else {
        System.out.println("Database directory does not exist!");
        File parentDir = dbDir.getParentFile();
        if (parentDir != null) {
          System.out.println("Parent directory: " + parentDir.getAbsolutePath());
          System.out.println("Parent directory exists: " + parentDir.exists());
        }
      }
      System.out.println("=== End Database Initialization Information ===");

      System.out.println("=== Initializing Database-Only Configuration ===");
      DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
      beanFactory.setAllowCircularReferences(false);
      TronApplicationContext context = new TronApplicationContext(beanFactory);
      context.register(DefaultConfig.class);
      context.refresh();
      System.out.println("Database-only context initialized successfully");

      // Check internal transaction configuration
      boolean saveInternalTx = CommonParameter.getInstance().isSaveInternalTx();
      boolean saveFeaturedInternalTx = CommonParameter.getInstance().isSaveFeaturedInternalTx();
      System.out.println("=== Internal Transaction Configuration ===");
      System.out.println("Save Internal Transactions: " + saveInternalTx);
      System.out.println("Save Featured Internal Transactions: " + saveFeaturedInternalTx);
      if (!saveInternalTx) {
        System.out.println("WARNING: Internal transactions are not being saved!");
        System.out.println("To enable internal transaction saving, set 'vm.saveInternalTx = true' in config.conf");
        System.out.println("Internal transactions will not appear in TransactionInfo output.");
      }

      // Skip full application startup for database-only operation
      // Application appT = ApplicationFactory.create(context);
      // appT.startup();

      // Print additional database status after initialization
      System.out.println("=== Post-Initialization Database Status ===");
      System.out.println("Application startup completed successfully");

      // Get ChainBaseManager and Wallet instances
      ChainBaseManager chainBaseManager = context.getBean(ChainBaseManager.class);
      Wallet wallet = context.getBean(Wallet.class);

      System.out.println("ChainBaseManager initialized: " + (chainBaseManager != null));
      System.out.println("Wallet initialized: " + (wallet != null));

      // Check if database stores are accessible
      try {
        boolean blockStoreEmpty = chainBaseManager.getBlockStore().isNotEmpty();
        System.out.println("BlockStore is not empty: " + blockStoreEmpty);
      } catch (Exception e) {
        System.out.println("Error checking BlockStore: " + e.getMessage());
      }

      try {
        boolean dynamicPropsStoreAccessible = chainBaseManager.getDynamicPropertiesStore() != null;
        System.out.println("DynamicPropertiesStore accessible: " + dynamicPropsStoreAccessible);
      } catch (Exception e) {
        System.out.println("Error accessing DynamicPropertiesStore: " + e.getMessage());
      }

      System.out.println("=== End Post-Initialization Database Status ===");

      // Get database block range using multiple approaches for robustness
      long lowestBlockNum = 0;
      long latestBlockNum = 0;

      // Try to get the lowest block number
      try {
        lowestBlockNum = chainBaseManager.getLowestBlockNum();

        // Check if this is a lite node (doesn't have genesis block)
        boolean isLiteNode = chainBaseManager.isLiteNode();
        System.out.println("Node type: " + (isLiteNode ? "Lite Node" : "Full Node"));

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
              System.out.println("Genesis block found: " + genesisBlock.getBlockId().toString());
              // For full nodes, lowest block should be 0 if genesis block exists
              lowestBlockNum = 0;
            }
          } catch (Exception genesisException) {
            System.out.println("Warning: Could not access genesis block: " + genesisException.getMessage());
            logger.warn("Could not access genesis block: {}", genesisException.getMessage());
          }
        } else {
          System.out.println("Lite node detected - genesis block (block 0) may not be available");
          logger.info("Lite node detected - lowest available block: {}", lowestBlockNum);
        }

      } catch (Exception e) {
        System.out.println("Warning: Could not get lowest block number, using 0. Error: " + e.getMessage());
        logger.warn("Could not get lowest block number: {}", e.getMessage());
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
      long batchSize = 100;
      for (long currentStart = startBlockNum; currentStart <= endBlockNum; currentStart += batchSize) {
        long currentEnd = Math.min(currentStart + batchSize - 1, endBlockNum);
        long limit = currentEnd - currentStart + 1;

        System.out.println("Fetching blocks from " + currentStart + " to " + currentEnd);

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
              System.out.println("Successfully retrieved genesis block directly");
              logger.info("Successfully retrieved genesis block directly");
            }
          } catch (Exception e) {
            System.out.println("Could not get genesis block directly: " + e.getMessage());
            logger.warn("Could not get genesis block directly: {}", e.getMessage());
          }

          // If we need more blocks after genesis block, get them from BlockStore
          if (limit > 1) {
            try {
              List<BlockCapsule> additionalBlocks = chainBaseManager.getBlockStore().getLimitNumber(1, limit - 1);
              blocks.addAll(additionalBlocks);
            } catch (Exception e) {
              System.out.println("Could not get blocks after genesis: " + e.getMessage());
              logger.warn("Could not get blocks after genesis: {}", e.getMessage());
            }
          }

          // If we still don't have any blocks, try the normal approach
          if (blocks.isEmpty()) {
            try {
              blocks = chainBaseManager.getBlockStore().getLimitNumber(currentStart, limit);
            } catch (Exception e) {
              System.out.println("Could not get blocks using normal approach: " + e.getMessage());
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

        // Process each block
        for (BlockCapsule block : blocks) {
          long blockNum = block.getNum();
          String blockId = block.getBlockId().toString();
          long timestamp = block.getTimeStamp();
          String formattedTime = dateFormat.format(new Date(timestamp));

          System.out.println("\nBlock #" + blockNum + 
                            " | ID: " + blockId + 
                            " | Time: " + formattedTime +
                            " | Transactions: " + block.getTransactions().size());

          // Process transactions in the block
          List<TransactionCapsule> transactions = block.getTransactions();
          totalTransactions += transactions.size();
          batchTransactions += transactions.size();

          // Log block processing details
          String blockProcessMessage = String.format("Processing Block #%d with %d transactions",
                                                    blockNum, transactions.size());
          logger.debug(blockProcessMessage);

          if (transactions.isEmpty()) {
            System.out.println("  No transactions in this block");
          } else {
            for (int i = 0; i < transactions.size(); i++) {
              TransactionCapsule trx = transactions.get(i);
              String txId = trx.getTransactionId().toString();
              ByteString txIdBytes = ByteString.copyFrom(ByteArray.fromHexString(txId));

              System.out.println("  Transaction #" + (i + 1) + ":");

              // Get transaction info using wallet.getTransactionInfoById
              TransactionInfo transactionInfo = wallet.getTransactionInfoById(txIdBytes);

              // Get transaction using wallet.getTransactionById (walletsolidity/gettransactionbyid equivalent)
              Transaction transaction = wallet.getTransactionById(txIdBytes);

              // For genesis block or other special cases, if wallet methods return null,
              // try to get transaction directly from the TransactionCapsule
              if (transaction == null && trx != null) {
                try {
                  transaction = trx.getInstance();
                  System.out.println("    Retrieved transaction directly from TransactionCapsule");
                  logger.debug("Retrieved transaction directly from TransactionCapsule for ID: {}", txId);
                } catch (Exception e) {
                  System.out.println("    Could not get transaction from TransactionCapsule: " + e.getMessage());
                  logger.warn("Could not get transaction from TransactionCapsule for ID {}: {}", txId, e.getMessage());
                }
              }

              // Handle different output formats
              if ("trigger".equals(outputFormat)) {
                // Use TransactionLogTrigger format with optional Kafka sending
                String kafkaTopicToUse = useKafka ? kafkaTopic : null;
                String kafkaKey = txId; // Use transaction ID as Kafka key

                // Log if transaction data is missing
                if (transactionInfo == null && transaction == null) {
                  logger.warn("Missing transaction data for ID {} in block {} - this may be normal for genesis block transactions", txId, blockNum);
                  System.out.println("    Warning: No transaction data available from wallet methods");
                  if (blockNum == 0) {
                    System.out.println("    Note: This is normal for genesis block transactions");
                  }
                }

                printTransactionLogTrigger(transactionInfo, transaction, blockId, blockNum, timestamp, i,
                    "Transaction #" + (i + 1) + " (TransactionLogTrigger Format)", kafkaTopicToUse, kafkaKey);
              } else {
                // Use traditional formats
                if (transactionInfo != null) {
                  // Convert log addresses to TRON addresses while preserving internal transactions
                  List<Log> newLogList = Util.convertLogAddressToTronAddress(transactionInfo);
                  TransactionInfo transactionInfoWithConvertedLogs = transactionInfo.toBuilder()
                      .clearLog()
                      .addAllLog(newLogList)
                      .build();

                  System.out.println("    === Transaction Info (wallet/gettransactioninfobyid) ===");

                  if ("json".equals(outputFormat) || "both".equals(outputFormat)) {
                    System.out.println("    --- JSON Format ---");
                    System.out.println(JsonFormat.printToString(transactionInfoWithConvertedLogs, true));
                  }

                  if ("protobuf".equals(outputFormat) || "both".equals(outputFormat)) {
                    System.out.println("    --- Protobuf Format ---");
                    System.out.println(transactionInfoWithConvertedLogs.toString());
                  }
                } else {
                  System.out.println("    No transaction info found for ID: " + txId);
                }

                if (transaction != null) {
                  System.out.println("    === Transaction Details (walletsolidity/gettransactionbyid) ===");

                  if ("json".equals(outputFormat) || "both".equals(outputFormat)) {
                    System.out.println("    --- JSON Format ---");
                    System.out.println(JsonFormat.printToString(transaction, true));
                  }

                  if ("protobuf".equals(outputFormat) || "both".equals(outputFormat)) {
                    System.out.println("    --- Protobuf Format ---");
                    System.out.println(transaction.toString());
                  }
                } else {
                  System.out.println("    No transaction details found for ID: " + txId);
                }
              }
            }
          }
        }

        // Update statistics after processing each batch
        updateStatistics(blocks.size(), batchTransactions);
      }

      // Log and print final statistics
      logFinalStatistics();

      // Print summary
      System.out.println("\n=== Summary ===");
      System.out.println("Total blocks processed: " + totalBlocks);
      System.out.println("Total transactions: " + totalTransactions);
      double avgTxPerBlock = totalBlocks > 0 ? (double)totalTransactions / totalBlocks : 0;
      System.out.println("Average transactions per block: " + String.format("%.2f", avgTxPerBlock));

      // Log summary to file
      String summaryMessage = String.format("Processing completed - Total blocks: %d, Total transactions: %d, Average tx/block: %.2f",
                                          totalBlocks, totalTransactions, avgTxPerBlock);
      logger.info(summaryMessage);

      // Shutdown the context
      context.close();
      String completionMessage = "Transaction printing completed successfully.";
      System.out.println("\n" + completionMessage);
      logger.info(completionMessage);
      System.out.println("=== Read-Only Database Query Completed - Exiting Program ===");

    } catch (NumberFormatException e) {
      String errorMessage = "Error: Block numbers must be valid integers";
      System.out.println(errorMessage);
      logger.error(errorMessage, e);
    } catch (Exception e) {
      String errorMessage = "Error: " + e.getMessage();
      System.out.println(errorMessage);
      logger.error(errorMessage, e);
      e.printStackTrace();
    } finally {
      // Close Kafka producer if it was initialized
      closeKafkaProducer();
    }
  }
}
