package org.tron.program;

import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.tron.common.application.TronApplicationContext;
import org.tron.common.logsfilter.trigger.TransactionLogTrigger;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.JsonUtil;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.ChainBaseManager;
import org.tron.core.Constant;
import org.tron.core.Wallet;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.capsule.TransactionRetCapsule;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.core.db.common.iterator.DBIterator;
import org.tron.protos.Protocol.Transaction;
import org.tron.protos.Protocol.TransactionInfo;
import org.tron.protos.TransactionLogTriggerProtos;

import org.tron.program.exporter.KafkaSender;
import org.tron.program.exporter.ProcessingStats;
import org.tron.program.exporter.TransactionProcessor;
import org.tron.program.exporter.TriggerBuilder;
import org.tron.program.exporter.TriggerProtoConverter;

/**
 * A program to print/export transactions from a specified block range.
 *
 * Usage:
 *   BlockTransactionPrinter <startBlockNum> <endBlockNum> [options]
 *   BlockTransactionPrinter -tx <transactionId> [options]
 *
 * Options:
 *   -c <config_file>   Custom configuration file
 *   -d <data_dir>       Custom data directory
 *   -fm <format>        Output format (trigger|trigger-proto), default: trigger
 *   -kb <brokers>       Kafka broker addresses
 *   -kt <topic>         Kafka topic name
 *   -kafka-rate <rate>  Kafka rate limit in messages/second (0 = no limit)
 *   -threads <count>    Thread pool size, default: CPU cores * 2
 */
@Slf4j(topic = "app")
public class BlockTransactionPrinter {

  private static final int DEFAULT_THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;
  private static final long DEFAULT_BATCH_SIZE = 200;

  public static void main(String[] args) {
    logger.info("BlockTransactionPrinter started");

    if (args.length < 2) {
      printUsage();
      return;
    }

    // ---- Parse arguments ----
    String outputFormat = parseStringArg(args, "-fm", "trigger").toLowerCase();
    String kafkaBrokers = parseStringArg(args, "-kb", null);
    String kafkaTopic = parseStringArg(args, "-kt", null);
    int kafkaRateLimit = parseIntArg(args, "-kafka-rate", 0);
    int customThreadPoolSize = parseIntArg(args, "-threads", DEFAULT_THREAD_POOL_SIZE);

    if (customThreadPoolSize <= 0) {
      customThreadPoolSize = DEFAULT_THREAD_POOL_SIZE;
    }

    // ---- Validate ----
    if (!validateArgs(outputFormat, kafkaBrokers, kafkaTopic)) {
      return;
    }

    boolean useKafka = kafkaBrokers != null && kafkaTopic != null;

    // ---- Mode detection ----
    boolean isTransactionMode = "-tx".equals(args[0]);
    String transactionId = null;
    long startBlockNum = 0;
    long endBlockNum = 0;

    try {
      if (isTransactionMode) {
        transactionId = args[1];
        if (transactionId.length() != 64 || !transactionId.matches("[0-9a-fA-F]+")) {
          logger.error("Invalid transaction ID format. Expected 64-character hex string.");
          return;
        }
      } else {
        startBlockNum = Long.parseLong(args[0]);
        endBlockNum = Long.parseLong(args[1]);
        if (startBlockNum < 0 || (endBlockNum < 0 && endBlockNum != -1)
            || (endBlockNum >= 0 && startBlockNum > endBlockNum)) {
          logger.error("Invalid block range. Start block must be >= 0, "
              + "end block must be >= start block or -1 (latest)");
          return;
        }
      }
    } catch (NumberFormatException e) {
      logger.error("Block numbers must be valid integers", e);
      return;
    }

    // ---- Initialize modules ----
    KafkaSender kafkaSender = new KafkaSender();
    ProcessingStats stats = new ProcessingStats();
    TransactionProcessor processor = new TransactionProcessor(customThreadPoolSize);
    TronApplicationContext context = null;

    try {

      // Initialize Kafka
      if (useKafka) {
        if ("trigger-proto".equals(outputFormat)) {
          if (!kafkaSender.initBytesProducer(kafkaBrokers)) {
            logger.warn("Failed to initialize Kafka proto producer. Continuing without Kafka.");
            useKafka = false;
          }
        } else {
          if (!kafkaSender.initStringProducer(kafkaBrokers)) {
            logger.warn("Failed to initialize Kafka producer. Continuing without Kafka.");
            useKafka = false;
          }
        }
        if (useKafka && kafkaRateLimit > 0) {
          kafkaSender.setRateLimit(kafkaRateLimit);
        }
      }

      // ---- Initialize TRON environment ----
      String[] configArgs = buildConfigArgs(args);
      context = setupTronContext(configArgs);

      ChainBaseManager chainBaseManager = context.getBean(ChainBaseManager.class);
      Wallet wallet = context.getBean(Wallet.class);

      // ---- Get database block range ----
      long lowestBlockNum = getLowestBlockNum(chainBaseManager);
      long latestBlockNum = getLatestBlockNum(chainBaseManager);

      System.out.println("=== Database block range: " + lowestBlockNum + " to " + latestBlockNum + " ===");
      logger.info("Database block range: {} to {}", lowestBlockNum, latestBlockNum);

      if (latestBlockNum == 0 && lowestBlockNum == 0) {
        System.out.println("Warning: Database appears to be empty or not properly initialized.");
        return;
      }

      // Resolve -1 to the latest block number
      if (!isTransactionMode && endBlockNum == -1) {
        endBlockNum = latestBlockNum;
        logger.info("endBlockNum resolved to latest: {}", endBlockNum);
      }

      // ---- Transaction mode ----
      if (isTransactionMode) {
        handleTransactionMode(transactionId, outputFormat, useKafka, kafkaTopic,
            kafkaSender, wallet, chainBaseManager);
        System.out.println("=== Read-Only Database Query Completed - Exiting Program ===");
        return;
      }

      // ---- Block range validation ----
      if (startBlockNum < lowestBlockNum || endBlockNum > latestBlockNum) {
        System.out.println("Error: Requested block range (" + startBlockNum + " to " + endBlockNum
            + ") is outside the available range (" + lowestBlockNum + " to " + latestBlockNum + ")");
        return;
      }

      // Initialize stats only in block range mode
      stats.initialize(startBlockNum);

      System.out.println("=== Printing transactions from block " + startBlockNum + " to " + endBlockNum + " ===");

      // ---- Main processing loop ----
      int totalBlocks = 0;
      int totalTransactions = 0;
      long batchSize = DEFAULT_BATCH_SIZE;
      DBIterator blockIterator = null;
      DBIterator transactionRetIterator = null;

      try {
        blockIterator = openIterator(chainBaseManager.getBlockStore().getDb().iterator(),
            createBlockSeekKey(startBlockNum));
        transactionRetIterator = openIterator(
            chainBaseManager.getTransactionRetStore().getDb().iterator(),
            ByteArray.fromLong(startBlockNum));

        for (long currentStart = startBlockNum; currentStart <= endBlockNum; currentStart += batchSize) {
          long currentEnd = Math.min(currentStart + batchSize - 1, endBlockNum);
          long limit = currentEnd - currentStart + 1;

          long fetchStartTime = System.nanoTime();
          List<BlockCapsule> blocks;
          if (currentStart == 0) {
            blocks = fetchBlocks(chainBaseManager, currentStart, limit);
            if (blockIterator != null) {
              blockIterator.seek(createBlockSeekKey(currentEnd + 1));
            }
          } else {
            blocks = fetchBlocks(chainBaseManager, currentStart, limit, blockIterator);
          }
          long fetchBlocksDurationMs = nanosToMillis(System.nanoTime() - fetchStartTime);
          totalBlocks += blocks.size();

          // Batch range scan: 1 sequential iterator scan replaces N point lookups
          Map<Long, TransactionRetCapsule> transactionRetMap = new HashMap<>();
          long prefetchStartTime = System.nanoTime();
          if (!blocks.isEmpty()) {
            long firstBlockNum = blocks.get(0).getNum();
            long lastBlockNum = blocks.get(blocks.size() - 1).getNum();
            try {
              transactionRetMap = fetchTransactionRets(transactionRetIterator,
                  firstBlockNum, lastBlockNum);
            } catch (Exception e) {
              logger.warn("Failed to batch prefetch TransactionRetCapsule [{}, {}]: {}",
                  firstBlockNum, lastBlockNum, e.getMessage());
            }
          }
          long prefetchDurationMs = nanosToMillis(System.nanoTime() - prefetchStartTime);

          AtomicInteger batchTransactions = new AtomicInteger(0);
          AtomicLong lastBlockInBatch = new AtomicLong(currentStart);
          int nonEmptyBlockCount = 0;
          int prefetchedTransactionInfoCount = countTransactionInfos(transactionRetMap);
          BatchProcessingProfile batchProfile = new BatchProcessingProfile();
          long processingStartTime = System.nanoTime();

          // Concurrent block processing
          List<CompletableFuture<Void>> blockFutures = new ArrayList<>();
          final String fOutputFormat = outputFormat;
          final boolean fUseKafka = useKafka;
          final String fKafkaTopic = kafkaTopic;

          for (BlockCapsule block : blocks) {
            final long blockNum = block.getNum();
            final String blockId = block.getBlockId().toString();
            final long timestamp = block.getTimeStamp();
            final List<TransactionCapsule> transactions = block.getTransactions();
            if (!transactions.isEmpty()) {
              nonEmptyBlockCount++;
            }
            final TransactionRetCapsule prefetchedRet =
                transactionRetMap.get(blockNum);

            CompletableFuture<Void> blockFuture = CompletableFuture.runAsync(() -> {
              try {
                TransactionProcessor.BlockProcessingProfile blockProfile =
                    processor.processTransactionsConcurrently(transactions, blockId, blockNum, timestamp,
                    fOutputFormat, fUseKafka, fKafkaTopic, wallet, chainBaseManager, kafkaSender,
                    prefetchedRet);
                batchProfile.add(blockProfile);
                batchTransactions.addAndGet(transactions.size());
                lastBlockInBatch.set(blockNum);
              } catch (Exception e) {
                logger.error("Error processing block {}: {}", blockNum, e.getMessage(), e);
              }
            }, processor.getBlockExecutor());

            blockFutures.add(blockFuture);
          }

          try {
            CompletableFuture.allOf(blockFutures.toArray(new CompletableFuture[0])).get();
          } catch (Exception e) {
            logger.error("Error waiting for block batch processing: {}", e.getMessage(), e);
          }

          long processingDurationMs = nanosToMillis(System.nanoTime() - processingStartTime);
          totalTransactions += batchTransactions.get();
          stats.update(blocks.size(), batchTransactions.get(), lastBlockInBatch.get());
          logger.info(
              "Batch [{}-{}] | fetched {} blocks ({} non-empty, {} txs) | fetch={}ms, txRetPrefetch={}ms, process={}ms, prefetchedRetBlocks={}, prefetchedTxInfo={} | processDetail: index={}ms, wait={}ms, txWallSum={}ms, txCapsule={}ms, txInfoFallback={}ms/{} hit={}, txFallback={}ms/{} hit={}, txRetFallback={}ms/{} hit={}, historyPrefetch={}ms/{} infos={}, trigger={}ms, serialize={}ms, emit={}ms, prefetchHit={}, prefetchMiss={}, missingTxInfo={}, missingTx={}, txErrors={}, json={}, proto={}, legacy={}, kafkaStr={}, kafkaBytes={}, stdout={}, invalidTxId={}, nullJson={}, slowestBlock={}({} tx, {}ms)",
              currentStart, currentEnd, blocks.size(), nonEmptyBlockCount, batchTransactions.get(),
              fetchBlocksDurationMs, prefetchDurationMs, processingDurationMs,
              transactionRetMap.size(), prefetchedTransactionInfoCount,
              nanosToMillis(batchProfile.getIndexBuildNanos()),
              nanosToMillis(batchProfile.getFutureWaitNanos()),
              nanosToMillis(batchProfile.getTransactionWallNanos()),
              nanosToMillis(batchProfile.getTransactionCapsuleReadNanos()),
              nanosToMillis(batchProfile.getTransactionInfoFallbackQueryNanos()),
              batchProfile.getTransactionInfoFallbackQueryCount(),
              batchProfile.getTransactionInfoFallbackHitCount(),
              nanosToMillis(batchProfile.getTransactionFallbackQueryNanos()),
              batchProfile.getTransactionFallbackQueryCount(),
              batchProfile.getTransactionFallbackHitCount(),
              nanosToMillis(batchProfile.getTransactionRetFallbackReadNanos()),
              batchProfile.getTransactionRetFallbackReadCount(),
              batchProfile.getTransactionRetFallbackHitCount(),
              nanosToMillis(batchProfile.getTransactionHistoryPrefetchNanos()),
              batchProfile.getTransactionHistoryPrefetchCount(),
              batchProfile.getTransactionHistoryPrefetchInfoCount(),
              nanosToMillis(batchProfile.getTriggerBuildNanos()),
              nanosToMillis(batchProfile.getSerializationNanos()),
              nanosToMillis(batchProfile.getOutputNanos()),
              batchProfile.getPrefetchedTransactionInfoHitCount(),
              batchProfile.getPrefetchedTransactionInfoMissCount(),
              batchProfile.getMissingTransactionInfoCount(),
              batchProfile.getMissingTransactionCount(),
              batchProfile.getTransactionErrorCount(),
              batchProfile.getTriggerJsonCount(),
              batchProfile.getTriggerProtoCount(),
              batchProfile.getLegacyOutputCount(),
              batchProfile.getKafkaStringSendCount(),
              batchProfile.getKafkaBytesSendCount(),
              batchProfile.getStdoutOutputCount(),
              batchProfile.getInvalidTransactionIdSkipCount(),
              batchProfile.getSerializationNullCount(),
              batchProfile.getSlowestBlockNum(),
              batchProfile.getSlowestBlockTxCount(),
              nanosToMillis(batchProfile.getSlowestBlockWallNanos()));
        }
      } finally {
        closeQuietly(blockIterator);
        closeQuietly(transactionRetIterator);
      }

      stats.logFinal();
      logger.info("Processing completed - Total blocks: {}, Total transactions: {}", totalBlocks, totalTransactions);

    } catch (Exception e) {
      logger.error("Error: {}", e.getMessage(), e);
    } finally {
      processor.close();
      kafkaSender.close();
      if (context != null) {
        context.close();
      }
    }
  }

  // ========================================================================
  // Transaction mode handler
  // ========================================================================

  private static void handleTransactionMode(String transactionId, String outputFormat,
      boolean useKafka, String kafkaTopic, KafkaSender kafkaSender,
      Wallet wallet, ChainBaseManager chainBaseManager) {

    System.out.println("=== Querying transaction: " + transactionId + " ===");

    try {
      ByteString txIdBytes = ByteString.copyFrom(ByteArray.fromHexString(transactionId));
      TransactionInfo transactionInfo = wallet.getTransactionInfoById(txIdBytes);
      Transaction transaction = wallet.getTransactionById(txIdBytes);

      if (transactionInfo == null && transaction == null) {
        System.out.println("Transaction not found: " + transactionId);
        return;
      }

      String blockHash = "N/A";
      long blockNumber = 0;
      long timestamp = 0;

      if (transactionInfo != null) {
        blockNumber = transactionInfo.getBlockNumber();
        timestamp = transactionInfo.getBlockTimeStamp();
        try {
          BlockCapsule blockCapsule = chainBaseManager.getBlockByNum(blockNumber);
          if (blockCapsule != null) {
            blockHash = blockCapsule.getBlockId().toString();
          }
        } catch (Exception e) {
          logger.warn("Could not retrieve block hash for block {}: {}", blockNumber, e.getMessage());
        }
      }

      TransactionLogTrigger trigger = TriggerBuilder.createTransactionLogTrigger(
          transactionInfo, transaction, blockHash, blockNumber, timestamp, 0, null);

      if ("trigger-proto".equals(outputFormat)) {
        TransactionLogTriggerProtos.TransactionLogTriggerPB protoMsg =
            TriggerProtoConverter.convert(trigger);
        byte[] payload = protoMsg.toByteArray();

        if (useKafka && kafkaSender.hasBytesProducer()) {
          kafkaSender.sendBytes(kafkaTopic, transactionId, payload);
        } else {
          System.out.println(java.util.Base64.getEncoder().encodeToString(payload));
        }
      } else {
        String jsonOutput = JsonUtil.obj2Json(trigger);
        if (jsonOutput != null) {
          if (useKafka && kafkaSender.hasStringProducer()) {
            kafkaSender.send(kafkaTopic, transactionId, jsonOutput);
          } else {
            System.out.println(jsonOutput);
          }
        }
      }
    } catch (Exception e) {
      logger.error("Error querying transaction: {}", e.getMessage(), e);
    }
  }

  // ========================================================================
  // TRON environment setup
  // ========================================================================

  private static TronApplicationContext setupTronContext(String[] configArgs) {
    CommonParameter.getInstance().setNeedToUpdateAsset(false);
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

    System.setProperty("database.readonly", "true");
    System.setProperty("storage.readonly", "true");

    Args.setParam(configArgs, Constant.TESTNET_CONF);

    if (Args.getInstance().getStorage() != null) {
      Args.getInstance().getStorage().setDbSync(false);
      Args.getInstance().getStorage().setMaxFlushCount(0);
    }

    String databasePath = Args.getInstance().getOutputDirectory();
    logger.info("Database directory: {}", databasePath);

    File dbDir = new File(databasePath);
    if (!dbDir.exists()) {
      throw new RuntimeException("Database directory does not exist: " + databasePath);
    }

    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    beanFactory.setAllowCircularReferences(false);
    TronApplicationContext context = new TronApplicationContext(beanFactory);
    context.register(DefaultConfig.class);
    context.refresh();
    logger.info("Database context initialized successfully");
    return context;
  }

  // ========================================================================
  // Block fetching helpers
  // ========================================================================

  private static List<BlockCapsule> fetchBlocks(ChainBaseManager chainBaseManager,
      long currentStart, long limit, DBIterator blockIterator) {
    if (blockIterator != null) {
      return fetchBlocksFromIterator(blockIterator, currentStart, limit);
    }
    return fetchBlocks(chainBaseManager, currentStart, limit);
  }

  private static List<BlockCapsule> fetchBlocks(ChainBaseManager chainBaseManager,
      long currentStart, long limit) {
    if (currentStart == 0) {
      List<BlockCapsule> blocks = new ArrayList<>();
      try {
        BlockCapsule genesisBlock = chainBaseManager.getGenesisBlock();
        if (genesisBlock != null && genesisBlock.getNum() == 0) {
          blocks.add(genesisBlock);
        }
      } catch (Exception e) {
        logger.warn("Could not get genesis block directly: {}", e.getMessage());
      }

      if (limit > 1) {
        try {
          blocks.addAll(chainBaseManager.getBlockStore().getLimitNumber(1, limit - 1));
        } catch (Exception e) {
          logger.warn("Could not get blocks after genesis: {}", e.getMessage());
        }
      }

      if (blocks.isEmpty()) {
        try {
          blocks = chainBaseManager.getBlockStore().getLimitNumber(currentStart, limit);
        } catch (Exception e) {
          logger.warn("Could not get blocks: {}", e.getMessage());
          blocks = new ArrayList<>();
        }
      }
      return blocks;
    }
    return chainBaseManager.getBlockStore().getLimitNumber(currentStart, limit);
  }

  private static List<BlockCapsule> fetchBlocksFromIterator(DBIterator blockIterator,
      long currentStart, long limit) {
    List<BlockCapsule> blocks = new ArrayList<>();
    long currentEnd = currentStart + limit - 1;

    while (blockIterator.valid() && blocks.size() < limit) {
      boolean advanceIterator = true;
      try {
        BlockCapsule block = new BlockCapsule(blockIterator.getValue());
        long blockNum = block.getNum();
        if (blockNum < currentStart) {
          continue;
        }
        if (blockNum > currentEnd) {
          advanceIterator = false;
          break;
        }
        blocks.add(block);
      } catch (Exception e) {
        logger.warn("Could not parse block near {}: {}", currentStart, e.getMessage());
      } finally {
        if (advanceIterator) {
          blockIterator.next();
        }
      }
    }

    return blocks;
  }

  private static Map<Long, TransactionRetCapsule> fetchTransactionRets(DBIterator transactionRetIterator,
      long startBlock, long endBlock) {
    Map<Long, TransactionRetCapsule> result = new HashMap<>();
    if (transactionRetIterator == null || endBlock < startBlock) {
      return result;
    }

    while (transactionRetIterator.valid()) {
      long blockNum = ByteArray.toLong(transactionRetIterator.getKey());
      if (blockNum < startBlock) {
        transactionRetIterator.next();
        continue;
      }
      if (blockNum > endBlock) {
        break;
      }

      try {
        result.put(blockNum, new TransactionRetCapsule(transactionRetIterator.getValue()));
      } catch (Exception e) {
        logger.warn("Skipping malformed TransactionRetCapsule for block {}: {}", blockNum,
            e.getMessage());
      } finally {
        transactionRetIterator.next();
      }
    }

    return result;
  }

  private static int countTransactionInfos(Map<Long, TransactionRetCapsule> transactionRetMap) {
    int total = 0;
    for (TransactionRetCapsule capsule : transactionRetMap.values()) {
      if (capsule != null && capsule.getInstance() != null) {
        total += capsule.getInstance().getTransactioninfoList().size();
      }
    }
    return total;
  }

  private static DBIterator openIterator(java.util.Iterator<Map.Entry<byte[], byte[]>> iterator,
      byte[] startKey) {
    if (!(iterator instanceof DBIterator)) {
      return null;
    }
    DBIterator dbIterator = (DBIterator) iterator;
    dbIterator.seek(startKey);
    return dbIterator;
  }

  private static byte[] createBlockSeekKey(long blockNum) {
    return new BlockCapsule.BlockId(Sha256Hash.ZERO_HASH, blockNum).getBytes();
  }

  private static void closeQuietly(DBIterator iterator) {
    if (iterator == null) {
      return;
    }
    try {
      iterator.close();
    } catch (IOException e) {
      logger.warn("Failed to close iterator: {}", e.getMessage());
    }
  }

  private static final class BatchProcessingProfile {

    private final LongAdder indexBuildNanos = new LongAdder();
    private final LongAdder futureWaitNanos = new LongAdder();
    private final LongAdder transactionWallNanos = new LongAdder();
    private final LongAdder transactionCapsuleReadNanos = new LongAdder();
    private final LongAdder transactionInfoFallbackQueryNanos = new LongAdder();
    private final LongAdder transactionFallbackQueryNanos = new LongAdder();
    private final LongAdder transactionRetFallbackReadNanos = new LongAdder();
    private final LongAdder transactionHistoryPrefetchNanos = new LongAdder();
    private final LongAdder triggerBuildNanos = new LongAdder();
    private final LongAdder serializationNanos = new LongAdder();
    private final LongAdder outputNanos = new LongAdder();
    private final AtomicInteger prefetchedTransactionInfoHitCount = new AtomicInteger();
    private final AtomicInteger prefetchedTransactionInfoMissCount = new AtomicInteger();
    private final AtomicInteger transactionInfoFallbackQueryCount = new AtomicInteger();
    private final AtomicInteger transactionInfoFallbackHitCount = new AtomicInteger();
    private final AtomicInteger transactionFallbackQueryCount = new AtomicInteger();
    private final AtomicInteger transactionFallbackHitCount = new AtomicInteger();
    private final AtomicInteger transactionRetFallbackReadCount = new AtomicInteger();
    private final AtomicInteger transactionRetFallbackHitCount = new AtomicInteger();
    private final AtomicInteger transactionHistoryPrefetchCount = new AtomicInteger();
    private final AtomicInteger transactionHistoryPrefetchInfoCount = new AtomicInteger();
    private final AtomicInteger missingTransactionInfoCount = new AtomicInteger();
    private final AtomicInteger missingTransactionCount = new AtomicInteger();
    private final AtomicInteger transactionErrorCount = new AtomicInteger();
    private final AtomicInteger triggerJsonCount = new AtomicInteger();
    private final AtomicInteger triggerProtoCount = new AtomicInteger();
    private final AtomicInteger legacyOutputCount = new AtomicInteger();
    private final AtomicInteger kafkaStringSendCount = new AtomicInteger();
    private final AtomicInteger kafkaBytesSendCount = new AtomicInteger();
    private final AtomicInteger stdoutOutputCount = new AtomicInteger();
    private final AtomicInteger invalidTransactionIdSkipCount = new AtomicInteger();
    private final AtomicInteger serializationNullCount = new AtomicInteger();
    private long slowestBlockNum = -1;
    private int slowestBlockTxCount = 0;
    private long slowestBlockWallNanos = 0;

    public synchronized void add(TransactionProcessor.BlockProcessingProfile profile) {
      indexBuildNanos.add(profile.getIndexBuildNanos());
      futureWaitNanos.add(profile.getFutureWaitNanos());
      transactionWallNanos.add(profile.getTransactionWallNanos());
      transactionCapsuleReadNanos.add(profile.getTransactionCapsuleReadNanos());
      transactionInfoFallbackQueryNanos.add(profile.getTransactionInfoFallbackQueryNanos());
      transactionFallbackQueryNanos.add(profile.getTransactionFallbackQueryNanos());
      transactionRetFallbackReadNanos.add(profile.getTransactionRetFallbackReadNanos());
      transactionHistoryPrefetchNanos.add(profile.getTransactionHistoryPrefetchNanos());
      triggerBuildNanos.add(profile.getTriggerBuildNanos());
      serializationNanos.add(profile.getSerializationNanos());
      outputNanos.add(profile.getOutputNanos());
      prefetchedTransactionInfoHitCount.addAndGet(profile.getPrefetchedTransactionInfoHitCount());
      prefetchedTransactionInfoMissCount.addAndGet(profile.getPrefetchedTransactionInfoMissCount());
      transactionInfoFallbackQueryCount.addAndGet(profile.getTransactionInfoFallbackQueryCount());
      transactionInfoFallbackHitCount.addAndGet(profile.getTransactionInfoFallbackHitCount());
      transactionFallbackQueryCount.addAndGet(profile.getTransactionFallbackQueryCount());
      transactionFallbackHitCount.addAndGet(profile.getTransactionFallbackHitCount());
      transactionRetFallbackReadCount.addAndGet(profile.getTransactionRetFallbackReadCount());
      transactionRetFallbackHitCount.addAndGet(profile.getTransactionRetFallbackHitCount());
      transactionHistoryPrefetchCount.addAndGet(profile.getTransactionHistoryPrefetchCount());
      transactionHistoryPrefetchInfoCount.addAndGet(profile.getTransactionHistoryPrefetchInfoCount());
      missingTransactionInfoCount.addAndGet(profile.getMissingTransactionInfoCount());
      missingTransactionCount.addAndGet(profile.getMissingTransactionCount());
      transactionErrorCount.addAndGet(profile.getTransactionErrorCount());
      triggerJsonCount.addAndGet(profile.getTriggerJsonCount());
      triggerProtoCount.addAndGet(profile.getTriggerProtoCount());
      legacyOutputCount.addAndGet(profile.getLegacyOutputCount());
      kafkaStringSendCount.addAndGet(profile.getKafkaStringSendCount());
      kafkaBytesSendCount.addAndGet(profile.getKafkaBytesSendCount());
      stdoutOutputCount.addAndGet(profile.getStdoutOutputCount());
      invalidTransactionIdSkipCount.addAndGet(profile.getInvalidTransactionIdSkipCount());
      serializationNullCount.addAndGet(profile.getSerializationNullCount());

      if (profile.getBlockWallNanos() > slowestBlockWallNanos) {
        slowestBlockWallNanos = profile.getBlockWallNanos();
        slowestBlockNum = profile.getBlockNum();
        slowestBlockTxCount = profile.getTransactionCount();
      }
    }

    public long getIndexBuildNanos() {
      return indexBuildNanos.sum();
    }

    public long getFutureWaitNanos() {
      return futureWaitNanos.sum();
    }

    public long getTransactionWallNanos() {
      return transactionWallNanos.sum();
    }

    public long getTransactionCapsuleReadNanos() {
      return transactionCapsuleReadNanos.sum();
    }

    public long getTransactionInfoFallbackQueryNanos() {
      return transactionInfoFallbackQueryNanos.sum();
    }

    public int getTransactionInfoFallbackQueryCount() {
      return transactionInfoFallbackQueryCount.get();
    }

    public int getTransactionInfoFallbackHitCount() {
      return transactionInfoFallbackHitCount.get();
    }

    public long getTransactionFallbackQueryNanos() {
      return transactionFallbackQueryNanos.sum();
    }

    public int getTransactionFallbackQueryCount() {
      return transactionFallbackQueryCount.get();
    }

    public int getTransactionFallbackHitCount() {
      return transactionFallbackHitCount.get();
    }

    public long getTransactionRetFallbackReadNanos() {
      return transactionRetFallbackReadNanos.sum();
    }

    public int getTransactionRetFallbackReadCount() {
      return transactionRetFallbackReadCount.get();
    }

    public int getTransactionRetFallbackHitCount() {
      return transactionRetFallbackHitCount.get();
    }

    public long getTransactionHistoryPrefetchNanos() {
      return transactionHistoryPrefetchNanos.sum();
    }

    public int getTransactionHistoryPrefetchCount() {
      return transactionHistoryPrefetchCount.get();
    }

    public int getTransactionHistoryPrefetchInfoCount() {
      return transactionHistoryPrefetchInfoCount.get();
    }

    public long getTriggerBuildNanos() {
      return triggerBuildNanos.sum();
    }

    public long getSerializationNanos() {
      return serializationNanos.sum();
    }

    public long getOutputNanos() {
      return outputNanos.sum();
    }

    public int getPrefetchedTransactionInfoHitCount() {
      return prefetchedTransactionInfoHitCount.get();
    }

    public int getPrefetchedTransactionInfoMissCount() {
      return prefetchedTransactionInfoMissCount.get();
    }

    public int getMissingTransactionInfoCount() {
      return missingTransactionInfoCount.get();
    }

    public int getMissingTransactionCount() {
      return missingTransactionCount.get();
    }

    public int getTransactionErrorCount() {
      return transactionErrorCount.get();
    }

    public int getTriggerJsonCount() {
      return triggerJsonCount.get();
    }

    public int getTriggerProtoCount() {
      return triggerProtoCount.get();
    }

    public int getLegacyOutputCount() {
      return legacyOutputCount.get();
    }

    public int getKafkaStringSendCount() {
      return kafkaStringSendCount.get();
    }

    public int getKafkaBytesSendCount() {
      return kafkaBytesSendCount.get();
    }

    public int getStdoutOutputCount() {
      return stdoutOutputCount.get();
    }

    public int getInvalidTransactionIdSkipCount() {
      return invalidTransactionIdSkipCount.get();
    }

    public int getSerializationNullCount() {
      return serializationNullCount.get();
    }

    public long getSlowestBlockNum() {
      return slowestBlockNum;
    }

    public int getSlowestBlockTxCount() {
      return slowestBlockTxCount;
    }

    public long getSlowestBlockWallNanos() {
      return slowestBlockWallNanos;
    }
  }

  private static long getLowestBlockNum(ChainBaseManager chainBaseManager) {
    try {
      long lowest = chainBaseManager.getLowestBlockNum();
      if (lowest < 0) {
        List<BlockCapsule> firstBlocks = chainBaseManager.getBlockStore().getLimitNumber(0, 1);
        return firstBlocks.isEmpty() ? 0 : firstBlocks.get(0).getNum();
      }
      if (!chainBaseManager.isLiteNode()) {
        try {
          if (chainBaseManager.getGenesisBlock() != null) {
            return 0;
          }
        } catch (Exception ignored) {
        }
      }
      return lowest;
    } catch (Exception e) {
      return 0;
    }
  }

  private static long getLatestBlockNum(ChainBaseManager chainBaseManager) {
    try {
      return chainBaseManager.getDynamicPropertiesStore().getLatestBlockHeaderNumber();
    } catch (Exception e) {
      try {
        long num = chainBaseManager.getDynamicPropertiesStore().getLatestBlockHeaderNumberFromDB();
        if (num >= 0) return num;
        List<BlockCapsule> latest = chainBaseManager.getBlockStore().getBlockByLatestNum(1);
        return latest.isEmpty() ? 0 : latest.get(0).getNum();
      } catch (Exception e2) {
        return 0;
      }
    }
  }

  // ========================================================================
  // Argument parsing helpers
  // ========================================================================

  private static String parseStringArg(String[] args, String flag, String defaultValue) {
    for (int i = 0; i < args.length - 1; i++) {
      if (flag.equals(args[i])) {
        return args[i + 1];
      }
    }
    return defaultValue;
  }

  private static int parseIntArg(String[] args, String flag, int defaultValue) {
    String value = parseStringArg(args, flag, null);
    if (value == null) return defaultValue;
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private static long nanosToMillis(long nanos) {
    return nanos / 1_000_000L;
  }

  private static boolean validateArgs(String outputFormat, String kafkaBrokers, String kafkaTopic) {
    if (!outputFormat.equals("trigger") && !outputFormat.equals("trigger-proto")) {
      logger.error("Invalid output format: {}. Use 'trigger' or 'trigger-proto'", outputFormat);
      return false;
    }
    if (kafkaBrokers != null && kafkaTopic == null) {
      logger.error("Kafka brokers specified but no topic provided. Use -kt to specify topic.");
      return false;
    }
    if (kafkaBrokers == null && kafkaTopic != null) {
      logger.error("Kafka topic specified but no brokers provided. Use -kb to specify brokers.");
      return false;
    }
    return true;
  }

  private static String[] buildConfigArgs(String[] args) {
    int configStartIndex = 2;
    List<String> filteredArgs = new ArrayList<>();
    for (int i = configStartIndex; i < args.length; i++) {
      if ("-fm".equals(args[i]) || "-kb".equals(args[i]) || "-kt".equals(args[i])
          || "-kafka-rate".equals(args[i]) || "-threads".equals(args[i])) {
        i++; // Skip parameter value
      } else {
        filteredArgs.add(args[i]);
      }
    }
    return filteredArgs.toArray(new String[0]);
  }

  private static void printUsage() {
    System.out.println("Usage:");
    System.out.println("  BlockTransactionPrinter <startBlockNum> <endBlockNum> [options]");
    System.out.println("  BlockTransactionPrinter -tx <transactionId> [options]");
    System.out.println("Options:");
    System.out.println("  -c <config_file>: Specify a custom configuration file");
    System.out.println("  -d <data_dir>: Specify a custom data directory");
    System.out.println("  -fm <format>: Output format (trigger|trigger-proto), default: trigger");
    System.out.println("  -kb <brokers>: Kafka broker addresses");
    System.out.println("  -kt <topic>: Kafka topic name");
    System.out.println("  -kafka-rate <rate>: Kafka rate limit in messages/second (0 = no limit)");
    System.out.println("  -threads <count>: Thread pool size, default: " + DEFAULT_THREAD_POOL_SIZE);
  }
}
