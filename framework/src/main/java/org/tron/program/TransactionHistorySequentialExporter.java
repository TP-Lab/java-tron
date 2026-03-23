package org.tron.program;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.tron.common.application.TronApplicationContext;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.utils.ByteArray;
import org.tron.core.ChainBaseManager;
import org.tron.core.Constant;
import org.tron.core.Wallet;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.capsule.TransactionInfoCapsule;
import org.tron.core.capsule.TransactionRetCapsule;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.core.db.common.iterator.DBIterator;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.exception.BadItemException;
import org.tron.core.store.TransactionHistoryStore;
import org.tron.program.exporter.KafkaSender;
import org.tron.program.exporter.ProcessingStats;
import org.tron.program.exporter.TransactionProcessor;
import org.tron.program.exporter.sequential.ExportBucketManifest;
import org.tron.program.exporter.sequential.HistoryShardLoader;
import org.tron.program.exporter.sequential.HistoryShardWriter;
import org.tron.protos.Protocol.TransactionInfo;

@Slf4j(topic = "app")
public class TransactionHistorySequentialExporter {

  private static final int DEFAULT_THREAD_POOL_SIZE =
      Runtime.getRuntime().availableProcessors() * 2;
  private static final long DEFAULT_BUCKET_BLOCKS = 10_000L;
  private static final long PREPARE_PROGRESS_INTERVAL = 1_000_000L;

  public static void main(String[] args) {
    logger.info("TransactionHistorySequentialExporter started");

    if (args.length < 2) {
      printUsage();
      return;
    }

    long startBlockNum;
    long endBlockNum;
    try {
      startBlockNum = Long.parseLong(args[0]);
      endBlockNum = Long.parseLong(args[1]);
      if (startBlockNum < 0 || (endBlockNum < 0 && endBlockNum != -1)
          || (endBlockNum >= 0 && startBlockNum > endBlockNum)) {
        logger.error("Invalid block range. Start block must be >= 0, end block must be >= start");
        return;
      }
    } catch (NumberFormatException e) {
      logger.error("Block numbers must be valid integers", e);
      return;
    }

    Phase phase;
    try {
      phase = Phase.from(parseStringArg(args, "-phase", "all"));
    } catch (IllegalArgumentException e) {
      logger.error(e.getMessage());
      printUsage();
      return;
    }

    String outputFormat = parseStringArg(args, "-fm", "trigger").toLowerCase();
    String kafkaBrokers = parseStringArg(args, "-kb", null);
    String kafkaTopic = parseStringArg(args, "-kt", null);
    int kafkaRateLimit = parseIntArg(args, "-kafka-rate", 0);
    int customThreadPoolSize = parseIntArg(args, "-threads", DEFAULT_THREAD_POOL_SIZE);
    long bucketBlockCount = parseLongArg(args, "-bucket-blocks", DEFAULT_BUCKET_BLOCKS);
    String customTempDir = parseStringArg(args, "-tmp", null);
    boolean keepTemp = hasFlag(args, "-keep-temp");

    if (customThreadPoolSize <= 0) {
      customThreadPoolSize = DEFAULT_THREAD_POOL_SIZE;
    }
    if (bucketBlockCount <= 0) {
      bucketBlockCount = DEFAULT_BUCKET_BLOCKS;
    }
    if (!validateArgs(outputFormat, kafkaBrokers, kafkaTopic)) {
      return;
    }

    if ((kafkaBrokers != null || kafkaTopic != null)
        && !"trigger".equals(outputFormat)
        && !"trigger-proto".equals(outputFormat)) {
      logger.warn("Kafka output only supports trigger or trigger-proto. Kafka will be disabled.");
      kafkaBrokers = null;
      kafkaTopic = null;
    }

    boolean useKafka = kafkaBrokers != null && kafkaTopic != null;
    KafkaSender kafkaSender = new KafkaSender();
    TransactionProcessor processor = new TransactionProcessor(customThreadPoolSize);
    ProcessingStats stats = new ProcessingStats();
    TronApplicationContext context = null;

    try {
      if (useKafka) {
        if ("trigger-proto".equals(outputFormat)) {
          if (!kafkaSender.initBytesProducer(kafkaBrokers)) {
            logger.warn("Failed to initialize Kafka proto producer. Continuing without Kafka.");
            useKafka = false;
          }
        } else if (!kafkaSender.initStringProducer(kafkaBrokers)) {
          logger.warn("Failed to initialize Kafka producer. Continuing without Kafka.");
          useKafka = false;
        }

        if (useKafka && kafkaRateLimit > 0) {
          kafkaSender.setRateLimit(kafkaRateLimit);
        }
      }

      context = setupTronContext(buildConfigArgs(args));
      ChainBaseManager chainBaseManager = context.getBean(ChainBaseManager.class);
      Wallet wallet = context.getBean(Wallet.class);

      long lowestBlockNum = getLowestBlockNum(chainBaseManager);
      long latestBlockNum = getLatestBlockNum(chainBaseManager);
      if (endBlockNum == -1) {
        endBlockNum = latestBlockNum;
      }
      if (startBlockNum < lowestBlockNum || endBlockNum > latestBlockNum) {
        logger.error("Requested block range [{}-{}] is outside database range [{}-{}]",
            startBlockNum, endBlockNum, lowestBlockNum, latestBlockNum);
        return;
      }

      File workingDir = resolveWorkingDir(customTempDir, Args.getInstance().getOutputDirectory(),
          startBlockNum, endBlockNum);
      ExportBucketManifest manifest = phase == Phase.EXPORT
          ? ExportBucketManifest.load(workingDir)
          : ExportBucketManifest.create(workingDir, startBlockNum, endBlockNum, bucketBlockCount);

      if (phase == Phase.EXPORT
          && (manifest.getStartBlockNum() != startBlockNum || manifest.getEndBlockNum() != endBlockNum)) {
        logger.warn("Loaded manifest range [{}-{}] differs from requested range [{}-{}]",
            manifest.getStartBlockNum(), manifest.getEndBlockNum(), startBlockNum, endBlockNum);
      }

      long exportStartBlockNum = Math.max(startBlockNum, manifest.getStartBlockNum());
      long exportEndBlockNum = Math.min(endBlockNum, manifest.getEndBlockNum());
      if ((phase == Phase.EXPORT || phase == Phase.ALL) && exportStartBlockNum > exportEndBlockNum) {
        throw new IllegalArgumentException(String.format(
            "Requested export range [%d-%d] does not overlap manifest range [%d-%d]",
            startBlockNum, endBlockNum, manifest.getStartBlockNum(), manifest.getEndBlockNum()));
      }

      logger.info("Sequential export phase={} manifestRange=[{}-{}] requestedRange=[{}-{}] "
              + "effectiveExportRange=[{}-{}] bucketBlocks={} tempDir={}",
          phase.name().toLowerCase(), manifest.getStartBlockNum(), manifest.getEndBlockNum(),
          startBlockNum, endBlockNum, exportStartBlockNum, exportEndBlockNum,
          manifest.getBucketBlockCount(), workingDir.getAbsolutePath());

      if (phase == Phase.PREPARE || phase == Phase.ALL) {
        PrepareStats prepareStats =
            prepareHistoryShards(chainBaseManager.getTransactionHistoryStore(), manifest);
        prepareStats.logSummary(manifest);
      }

      if (phase == Phase.EXPORT || phase == Phase.ALL) {
        ExportStats exportStats = exportBuckets(chainBaseManager, wallet, kafkaSender, processor,
            stats, manifest, exportStartBlockNum, exportEndBlockNum, outputFormat, useKafka,
            kafkaTopic);
        exportStats.logSummary();
        stats.logFinal();
      }

      if (!keepTemp) {
        logger.info("Temporary shard files retained at {}. Delete manually after verification.",
            workingDir.getAbsolutePath());
      }
    } catch (Exception e) {
      logger.error("TransactionHistorySequentialExporter failed: {}", e.getMessage(), e);
      throw new RuntimeException("TransactionHistorySequentialExporter failed", e);
    } finally {
      processor.close();
      kafkaSender.close();
      if (context != null) {
        context.close();
      }
    }
  }

  private static PrepareStats prepareHistoryShards(TransactionHistoryStore transactionHistoryStore,
      ExportBucketManifest manifest) throws Exception {
    logger.info("Prepare phase started - sequential scan transactionHistoryStore");

    PrepareStats prepareStats = new PrepareStats();

    DBIterator iterator = null;
    try (HistoryShardWriter shardWriter = new HistoryShardWriter(manifest)) {
      iterator = (DBIterator) transactionHistoryStore.getDb().iterator();
      iterator.seekToFirst();

      while (iterator.valid()) {
        byte[] value = iterator.getValue();
        prepareStats.recordScanned(value.length);

        try {
          TransactionInfoCapsule capsule = new TransactionInfoCapsule(value);
          long blockNum = capsule.getBlockNumber();
          if (blockNum < manifest.getStartBlockNum() || blockNum > manifest.getEndBlockNum()) {
            prepareStats.recordSkipped();
          } else if (shardWriter.append(blockNum, capsule.getId(), value)) {
            prepareStats.recordMatched(value.length);
          }
        } catch (BadItemException e) {
          prepareStats.recordMalformed();
          logger.warn("Skip malformed TransactionInfoCapsule during prepare: {}", e.getMessage());
        } finally {
          iterator.next();
        }

        if (prepareStats.shouldLogProgress()) {
          prepareStats.logProgress();
        }
      }
    } finally {
      closeQuietly(iterator);
    }

    return prepareStats;
  }

  private static ExportStats exportBuckets(ChainBaseManager chainBaseManager, Wallet wallet,
      KafkaSender kafkaSender, TransactionProcessor processor, ProcessingStats stats,
      ExportBucketManifest manifest, long exportStartBlockNum, long exportEndBlockNum,
      String outputFormat, boolean useKafka, String kafkaTopic) throws Exception {
    logger.info("Export phase started - replay blocks with prepared history shards "
            + "for effective range [{}-{}]", exportStartBlockNum, exportEndBlockNum);
    stats.initialize(exportStartBlockNum);

    HistoryShardLoader shardLoader = new HistoryShardLoader();
    ExportStats exportStats = new ExportStats();
    for (ExportBucketManifest.Bucket bucket : manifest.getBuckets()) {
      if (!bucketOverlapsRange(bucket, exportStartBlockNum, exportEndBlockNum)) {
        exportStats.recordFilteredBucket();
        continue;
      }

      if (!bucket.isPrepared()) {
        throw new IllegalStateException(String.format(
            "Unprepared bucket [%d-%d] detected during export. Run prepare first or verify the "
                + "manifest integrity.", bucket.getStartBlockNum(), bucket.getEndBlockNum()));
      }

      if (bucket.isExported()) {
        exportStats.recordSkippedExportedBucket();
        continue;
      }

      BucketExportStats bucketStats = new BucketExportStats(bucket);
      long bucketExportStart = Math.max(bucket.getStartBlockNum(), exportStartBlockNum);
      long bucketExportEnd = Math.min(bucket.getEndBlockNum(), exportEndBlockNum);
      logger.info("Export bucket start [{}-{}] | effectiveRange=[{}-{}] shardRecords={} "
              + "shardBytes={} exported={}",
          bucket.getStartBlockNum(), bucket.getEndBlockNum(), bucketExportStart, bucketExportEnd,
          bucket.getRecordCount(), formatBytes(bucket.getSerializedBytes()), bucket.isExported());

      long loadStartTime = System.currentTimeMillis();
      HistoryShardLoader.BucketShardData shardData =
          shardLoader.load(manifest.resolveShardFile(bucket));
      long loadDurationMs = System.currentTimeMillis() - loadStartTime;
      bucketStats.recordShardLoad(shardData.getRecordCount(), shardData.getPayloadBytes(),
          loadDurationMs);

      long fetchStartTime = System.currentTimeMillis();
      List<BlockCapsule> blocks = fetchBlocks(chainBaseManager, bucketExportStart,
          bucketExportEnd - bucketExportStart + 1);
      long fetchDurationMs = System.currentTimeMillis() - fetchStartTime;
      bucketStats.recordBlockFetch(blocks.size(), fetchDurationMs);
      long processStartTime = System.currentTimeMillis();

      for (BlockCapsule block : blocks) {
        List<TransactionCapsule> transactions = block.getTransactions();
        bucketStats.recordBlockScanned(transactions.size());
        if (transactions.isEmpty()) {
          stats.update(1, 0, block.getNum());
          continue;
        }

        PreparedBlockRet preparedBlockRet = resolveExportTransactionRet(block,
            shardData.getBlockInfos(block.getNum()), chainBaseManager, bucketStats);
        if (!preparedBlockRet.isComplete()) {
          bucketStats.recordIncompleteBlock(block.getNum(), transactions.size(),
              preparedBlockRet.getMissingTransactionCount());
          throw new IllegalStateException(String.format(
              "Incomplete export data for block %d: missingTx=%d txCount=%d. "
                  + "Neither prepared shard nor transactionRetStore can fully cover this block.",
              block.getNum(), preparedBlockRet.getMissingTransactionCount(), transactions.size()));
        }

        processor.processTransactionsConcurrently(transactions, block.getBlockId().toString(),
            block.getNum(), block.getTimeStamp(), outputFormat, useKafka, kafkaTopic, wallet,
            chainBaseManager, kafkaSender, preparedBlockRet.getTransactionRetCapsule());
        bucketStats.recordExportedBlock(transactions.size());
        stats.update(1, transactions.size(), block.getNum());
      }

      bucketStats.recordProcessTime(System.currentTimeMillis() - processStartTime);
      bucket.markExported();
      manifest.save();
      logger.info("Export bucket [{}-{}] | blocks={} txs={} shardRecords={} shardBytes={} "
              + "load={}ms fetch={}ms process={}ms emptyBlocks={} nonEmptyBlocks={} "
              + "exportedBlocks={} incompleteBlocks={} retFallbackBlocks={} retFallbackTxs={} "
              + "txCoverage={} infoCoverage={} missingTx={} "
              + "worstIncompleteBlock={}({} tx, missing={})",
          bucket.getStartBlockNum(), bucket.getEndBlockNum(), bucketStats.getBlocksFetched(),
          bucketStats.getTotalTransactions(), bucketStats.getShardRecordCount(),
          formatBytes(bucketStats.getShardPayloadBytes()), bucketStats.getShardLoadMillis(),
          bucketStats.getBlockFetchMillis(), bucketStats.getProcessMillis(),
          bucketStats.getEmptyBlocks(), bucketStats.getNonEmptyBlocks(),
          bucketStats.getExportedBlocks(), bucketStats.getIncompleteBlocks(),
          bucketStats.getTransactionRetFallbackBlocks(),
          bucketStats.getTransactionRetFallbackTransactions(),
          percentage(bucketStats.getExportedTransactions(), bucketStats.getTotalTransactions()),
          percentage(bucketStats.getShardRecordCount(), bucketStats.getTotalTransactions()),
          bucketStats.getMissingTransactions(), bucketStats.getWorstIncompleteBlockNum(),
          bucketStats.getWorstIncompleteBlockTxCount(), bucketStats.getWorstIncompleteMissingTx());
      exportStats.recordBucket(bucketStats);
    }

    return exportStats;
  }

  private static boolean bucketOverlapsRange(ExportBucketManifest.Bucket bucket,
      long exportStartBlockNum, long exportEndBlockNum) {
    return bucket.getEndBlockNum() >= exportStartBlockNum
        && bucket.getStartBlockNum() <= exportEndBlockNum;
  }

  private static PreparedBlockRet buildPreparedTransactionRet(BlockCapsule block,
      Map<WrappedByteArray, TransactionInfo> blockInfos) {
    List<TransactionCapsule> transactions = block.getTransactions();
    List<TransactionInfo> orderedInfos = new ArrayList<>(transactions.size());
    int missingTransactionCount = 0;

    for (TransactionCapsule transactionCapsule : transactions) {
      TransactionInfo transactionInfo = blockInfos.get(
          WrappedByteArray.of(transactionCapsule.getTransactionId().getBytes()));
      if (transactionInfo == null) {
        missingTransactionCount++;
        continue;
      }
      orderedInfos.add(transactionInfo);
    }

    if (missingTransactionCount > 0) {
      return PreparedBlockRet.incomplete(missingTransactionCount);
    }

    TransactionRetCapsule transactionRetCapsule = new TransactionRetCapsule(block);
    transactionRetCapsule.addAllTransactionInfos(orderedInfos);
    return PreparedBlockRet.complete(transactionRetCapsule);
  }

  private static PreparedBlockRet resolveExportTransactionRet(BlockCapsule block,
      Map<WrappedByteArray, TransactionInfo> blockInfos, ChainBaseManager chainBaseManager,
      BucketExportStats bucketStats) {
    PreparedBlockRet preparedBlockRet = buildPreparedTransactionRet(block, blockInfos);
    if (preparedBlockRet.isComplete()) {
      return preparedBlockRet;
    }

    TransactionRetCapsule fallbackRet = readTransactionRetFallback(block, chainBaseManager);
    if (fallbackRet == null) {
      return preparedBlockRet;
    }

    bucketStats.recordTransactionRetFallbackBlock(block.getTransactions().size());
    logger.info("Use transactionRetStore fallback for block={} missingPreparedTx={} txCount={}",
        block.getNum(), preparedBlockRet.getMissingTransactionCount(),
        block.getTransactions().size());
    return PreparedBlockRet.complete(fallbackRet);
  }

  private static TransactionRetCapsule readTransactionRetFallback(BlockCapsule block,
      ChainBaseManager chainBaseManager) {
    if (chainBaseManager == null || chainBaseManager.getTransactionRetStore() == null) {
      return null;
    }

    TransactionRetCapsule transactionRetCapsule;
    try {
      transactionRetCapsule = chainBaseManager.getTransactionRetStore()
          .getTransactionInfoByBlockNum(ByteArray.fromLong(block.getNum()));
    } catch (Exception e) {
      logger.warn("Failed to read transactionRetStore fallback for block {}: {}",
          block.getNum(), e.getMessage());
      return null;
    }

    if (transactionRetCapsule == null || transactionRetCapsule.getInstance() == null) {
      return null;
    }

    List<TransactionInfo> infos = transactionRetCapsule.getInstance().getTransactioninfoList();
    List<TransactionCapsule> transactions = block.getTransactions();
    Set<WrappedByteArray> txIds = new HashSet<>(transactions.size());
    for (TransactionCapsule transactionCapsule : transactions) {
      txIds.add(WrappedByteArray.of(transactionCapsule.getTransactionId().getBytes()));
    }

    for (TransactionInfo info : infos) {
      txIds.remove(WrappedByteArray.of(info.getId().toByteArray()));
    }

    if (!txIds.isEmpty()) {
      logger.warn("Ignore transactionRetStore fallback block={} retCount={} txCount={} missingTx={}",
          block.getNum(), infos.size(), transactions.size(), txIds.size());
      return null;
    }

    return transactionRetCapsule;
  }

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

    String databasePath = Args.getInstance().getOutputDirectory();
    File dbDir = new File(databasePath);
    if (!dbDir.exists()) {
      throw new RuntimeException("Database directory does not exist: " + databasePath);
    }

    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    beanFactory.setAllowCircularReferences(false);
    TronApplicationContext context = new TronApplicationContext(beanFactory);
    context.register(DefaultConfig.class);
    context.refresh();
    logger.info("Read-only database context initialized successfully");
    return context;
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
          // Ignore.
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
        long num = chainBaseManager.getDynamicPropertiesStore()
            .getLatestBlockHeaderNumberFromDB();
        if (num >= 0) {
          return num;
        }
        List<BlockCapsule> latest = chainBaseManager.getBlockStore().getBlockByLatestNum(1);
        return latest.isEmpty() ? 0 : latest.get(0).getNum();
      } catch (Exception e2) {
        return 0;
      }
    }
  }

  private static File resolveWorkingDir(String customTempDir, String outputDirectory,
      long startBlockNum, long endBlockNum) {
    if (customTempDir != null && !customTempDir.trim().isEmpty()) {
      return new File(customTempDir);
    }
    return new File(outputDirectory,
        String.format("sequential-export-%d-%d", startBlockNum, endBlockNum));
  }

  private static String[] buildConfigArgs(String[] args) {
    List<String> filteredArgs = new ArrayList<>();
    for (int i = 2; i < args.length; i++) {
      if ("-phase".equals(args[i]) || "-fm".equals(args[i]) || "-kb".equals(args[i])
          || "-kt".equals(args[i]) || "-kafka-rate".equals(args[i]) || "-threads".equals(args[i])
          || "-tmp".equals(args[i]) || "-bucket-blocks".equals(args[i])) {
        i++;
      } else if ("-keep-temp".equals(args[i])) {
        // flag only
      } else {
        filteredArgs.add(args[i]);
      }
    }
    return filteredArgs.toArray(new String[0]);
  }

  private static boolean validateArgs(String outputFormat, String kafkaBrokers, String kafkaTopic) {
    if (!"trigger".equals(outputFormat)
        && !"trigger-proto".equals(outputFormat)
        && !"json".equals(outputFormat)
        && !"protobuf".equals(outputFormat)
        && !"both".equals(outputFormat)) {
      logger.error("Unsupported format: {}", outputFormat);
      return false;
    }

    if ((kafkaBrokers == null) != (kafkaTopic == null)) {
      logger.error("Kafka configuration requires both -kb and -kt");
      return false;
    }

    return true;
  }

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
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      logger.warn("Invalid value for {}: {}. Using default {}", flag, value, defaultValue);
      return defaultValue;
    }
  }

  private static long parseLongArg(String[] args, String flag, long defaultValue) {
    String value = parseStringArg(args, flag, null);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      logger.warn("Invalid value for {}: {}. Using default {}", flag, value, defaultValue);
      return defaultValue;
    }
  }

  private static boolean hasFlag(String[] args, String flag) {
    for (String arg : args) {
      if (flag.equals(arg)) {
        return true;
      }
    }
    return false;
  }

  private static void printUsage() {
    System.out.println("Usage:");
    System.out.println("  TransactionHistorySequentialExporter <startBlockNum> <endBlockNum> [options]");
    System.out.println("  TransactionHistorySequentialExporter <startBlockNum> -1 [options]");
    System.out.println("Options:");
    System.out.println("  -c <config_file>: Specify a custom configuration file");
    System.out.println("  -d <data_dir>: Specify a custom data directory");
    System.out.println("  -phase <prepare|export|all>: Sequential export phase, default: all");
    System.out.println("  -tmp <temp_dir>: Temporary working directory");
    System.out.println("  -bucket-blocks <count>: Blocks per shard bucket, default: "
        + DEFAULT_BUCKET_BLOCKS);
    System.out.println("  -fm <format>: Output format (trigger|trigger-proto|json|protobuf|both)");
    System.out.println("  -kb <brokers>: Kafka broker addresses");
    System.out.println("  -kt <topic>: Kafka topic name");
    System.out.println("  -kafka-rate <rate>: Kafka rate limit in messages/second");
    System.out.println("  -threads <count>: Transaction processing thread pool size");
    System.out.println("  -keep-temp: Retain shard files after export");
    System.out.println("Notes:");
    System.out.println("  - endBlockNum = -1 means export to the latest block in the database");
  }

  private static void closeQuietly(DBIterator iterator) {
    if (iterator == null) {
      return;
    }
    try {
      iterator.close();
    } catch (Exception e) {
      logger.warn("Failed to close iterator: {}", e.getMessage());
    }
  }

  private enum Phase {
    PREPARE,
    EXPORT,
    ALL;

    static Phase from(String value) {
      if (value == null) {
        return ALL;
      }
      switch (value.toLowerCase()) {
        case "prepare":
          return PREPARE;
        case "export":
          return EXPORT;
        case "all":
          return ALL;
        default:
          throw new IllegalArgumentException("Unsupported phase: " + value);
      }
    }
  }

  private static final class PreparedBlockRet {
    private final TransactionRetCapsule transactionRetCapsule;
    private final int missingTransactionCount;

    private PreparedBlockRet(TransactionRetCapsule transactionRetCapsule,
        int missingTransactionCount) {
      this.transactionRetCapsule = transactionRetCapsule;
      this.missingTransactionCount = missingTransactionCount;
    }

    static PreparedBlockRet complete(TransactionRetCapsule transactionRetCapsule) {
      return new PreparedBlockRet(transactionRetCapsule, 0);
    }

    static PreparedBlockRet incomplete(int missingTransactionCount) {
      return new PreparedBlockRet(null, missingTransactionCount);
    }

    boolean isComplete() {
      return transactionRetCapsule != null;
    }

    TransactionRetCapsule getTransactionRetCapsule() {
      return transactionRetCapsule;
    }

    int getMissingTransactionCount() {
      return missingTransactionCount;
    }
  }

  private static final class PrepareStats {
    private final long startTimeMillis = System.currentTimeMillis();
    private long scannedRecords;
    private long matchedRecords;
    private long skippedRecords;
    private long malformedRecords;
    private long scannedBytes;
    private long matchedBytes;

    void recordScanned(long valueBytes) {
      scannedRecords++;
      scannedBytes += valueBytes;
    }

    void recordMatched(long valueBytes) {
      matchedRecords++;
      matchedBytes += valueBytes;
    }

    void recordSkipped() {
      skippedRecords++;
    }

    void recordMalformed() {
      malformedRecords++;
    }

    boolean shouldLogProgress() {
      return scannedRecords > 0 && scannedRecords % PREPARE_PROGRESS_INTERVAL == 0;
    }

    void logProgress() {
      long elapsedSeconds = elapsedSeconds();
      logger.info("Prepare progress | scanned={} matched={} skipped={} malformed={} "
              + "matchRate={} scannedBytes={} matchedBytes={} rate={} rec/s elapsed={}s",
          scannedRecords, matchedRecords, skippedRecords, malformedRecords,
          percentage(matchedRecords, scannedRecords), formatBytes(scannedBytes),
          formatBytes(matchedBytes), rate(scannedRecords, elapsedSeconds), elapsedSeconds);
    }

    void logSummary(ExportBucketManifest manifest) {
      long totalShardRecords = 0;
      long totalShardBytes = 0;
      int nonEmptyBuckets = 0;
      ExportBucketManifest.Bucket hottestBucket = null;

      for (ExportBucketManifest.Bucket bucket : manifest.getBuckets()) {
        totalShardRecords += bucket.getRecordCount();
        totalShardBytes += bucket.getSerializedBytes();
        if (bucket.getRecordCount() > 0) {
          nonEmptyBuckets++;
          if (hottestBucket == null
              || bucket.getRecordCount() > hottestBucket.getRecordCount()) {
            hottestBucket = bucket;
          }
        }
      }

      logger.info("Prepare summary | scanned={} matched={} skipped={} malformed={} "
              + "matchRate={} scannedBytes={} matchedBytes={} buckets={} nonEmptyBuckets={} "
              + "emptyBuckets={} shardRecords={} shardBytes={} avgMatchedBytesPerRecord={} "
              + "elapsed={}s",
          scannedRecords, matchedRecords, skippedRecords, malformedRecords,
          percentage(matchedRecords, scannedRecords), formatBytes(scannedBytes),
          formatBytes(matchedBytes), manifest.getBuckets().size(), nonEmptyBuckets,
          manifest.getBuckets().size() - nonEmptyBuckets, totalShardRecords,
          formatBytes(totalShardBytes),
          matchedRecords == 0 ? 0 : matchedBytes / matchedRecords, elapsedSeconds());

      if (hottestBucket != null) {
        logger.info("Prepare bucket hotspot | range=[{}-{}] records={} shardBytes={}",
            hottestBucket.getStartBlockNum(), hottestBucket.getEndBlockNum(),
            hottestBucket.getRecordCount(), formatBytes(hottestBucket.getSerializedBytes()));
      }
    }

    private long elapsedSeconds() {
      return Math.max(1, (System.currentTimeMillis() - startTimeMillis) / 1000);
    }
  }

  private static final class BucketExportStats {
    private final long startBlockNum;
    private final long endBlockNum;
    private long shardRecordCount;
    private long shardPayloadBytes;
    private long shardLoadMillis;
    private long blockFetchMillis;
    private long processMillis;
    private long blocksFetched;
    private long emptyBlocks;
    private long nonEmptyBlocks;
    private long exportedBlocks;
    private long incompleteBlocks;
    private long transactionRetFallbackBlocks;
    private long transactionRetFallbackTransactions;
    private long totalTransactions;
    private long exportedTransactions;
    private long missingTransactions;
    private long worstIncompleteBlockNum = -1;
    private int worstIncompleteMissingTx;
    private int worstIncompleteBlockTxCount;

    private BucketExportStats(ExportBucketManifest.Bucket bucket) {
      this.startBlockNum = bucket.getStartBlockNum();
      this.endBlockNum = bucket.getEndBlockNum();
    }

    void recordShardLoad(long shardRecordCount, long shardPayloadBytes, long shardLoadMillis) {
      this.shardRecordCount = shardRecordCount;
      this.shardPayloadBytes = shardPayloadBytes;
      this.shardLoadMillis = shardLoadMillis;
    }

    void recordBlockFetch(long blocksFetched, long blockFetchMillis) {
      this.blocksFetched = blocksFetched;
      this.blockFetchMillis = blockFetchMillis;
    }

    void recordBlockScanned(int transactionCount) {
      totalTransactions += transactionCount;
      if (transactionCount == 0) {
        emptyBlocks++;
      } else {
        nonEmptyBlocks++;
      }
    }

    void recordExportedBlock(int transactionCount) {
      exportedBlocks++;
      exportedTransactions += transactionCount;
    }

    void recordIncompleteBlock(long blockNum, int transactionCount, int missingTransactionCount) {
      incompleteBlocks++;
      missingTransactions += missingTransactionCount;
      if (missingTransactionCount > worstIncompleteMissingTx) {
        worstIncompleteMissingTx = missingTransactionCount;
        worstIncompleteBlockNum = blockNum;
        worstIncompleteBlockTxCount = transactionCount;
      }
    }

    void recordTransactionRetFallbackBlock(int transactionCount) {
      transactionRetFallbackBlocks++;
      transactionRetFallbackTransactions += transactionCount;
    }

    void recordProcessTime(long processMillis) {
      this.processMillis = processMillis;
    }

    long getStartBlockNum() {
      return startBlockNum;
    }

    long getEndBlockNum() {
      return endBlockNum;
    }

    long getShardRecordCount() {
      return shardRecordCount;
    }

    long getShardPayloadBytes() {
      return shardPayloadBytes;
    }

    long getShardLoadMillis() {
      return shardLoadMillis;
    }

    long getBlockFetchMillis() {
      return blockFetchMillis;
    }

    long getProcessMillis() {
      return processMillis;
    }

    long getBlocksFetched() {
      return blocksFetched;
    }

    long getEmptyBlocks() {
      return emptyBlocks;
    }

    long getNonEmptyBlocks() {
      return nonEmptyBlocks;
    }

    long getExportedBlocks() {
      return exportedBlocks;
    }

    long getIncompleteBlocks() {
      return incompleteBlocks;
    }

    long getTransactionRetFallbackBlocks() {
      return transactionRetFallbackBlocks;
    }

    long getTransactionRetFallbackTransactions() {
      return transactionRetFallbackTransactions;
    }

    long getTotalTransactions() {
      return totalTransactions;
    }

    long getExportedTransactions() {
      return exportedTransactions;
    }

    long getMissingTransactions() {
      return missingTransactions;
    }

    long getWorstIncompleteBlockNum() {
      return worstIncompleteBlockNum;
    }

    int getWorstIncompleteMissingTx() {
      return worstIncompleteMissingTx;
    }

    int getWorstIncompleteBlockTxCount() {
      return worstIncompleteBlockTxCount;
    }
  }

  private static final class ExportStats {
    private final long startTimeMillis = System.currentTimeMillis();
    private int filteredBuckets;
    private int skippedExportedBuckets;
    private int processedBuckets;
    private long shardRecords;
    private long shardPayloadBytes;
    private long shardLoadMillis;
    private long blockFetchMillis;
    private long processMillis;
    private long blocksFetched;
    private long emptyBlocks;
    private long nonEmptyBlocks;
    private long exportedBlocks;
    private long incompleteBlocks;
    private long transactionRetFallbackBlocks;
    private long transactionRetFallbackTransactions;
    private long totalTransactions;
    private long exportedTransactions;
    private long missingTransactions;
    private BucketExportStats weakestBucket;

    void recordFilteredBucket() {
      filteredBuckets++;
    }

    void recordSkippedExportedBucket() {
      skippedExportedBuckets++;
    }

    void recordBucket(BucketExportStats bucketStats) {
      processedBuckets++;
      shardRecords += bucketStats.getShardRecordCount();
      shardPayloadBytes += bucketStats.getShardPayloadBytes();
      shardLoadMillis += bucketStats.getShardLoadMillis();
      blockFetchMillis += bucketStats.getBlockFetchMillis();
      processMillis += bucketStats.getProcessMillis();
      blocksFetched += bucketStats.getBlocksFetched();
      emptyBlocks += bucketStats.getEmptyBlocks();
      nonEmptyBlocks += bucketStats.getNonEmptyBlocks();
      exportedBlocks += bucketStats.getExportedBlocks();
      incompleteBlocks += bucketStats.getIncompleteBlocks();
      transactionRetFallbackBlocks += bucketStats.getTransactionRetFallbackBlocks();
      transactionRetFallbackTransactions += bucketStats.getTransactionRetFallbackTransactions();
      totalTransactions += bucketStats.getTotalTransactions();
      exportedTransactions += bucketStats.getExportedTransactions();
      missingTransactions += bucketStats.getMissingTransactions();

      if (bucketStats.getTotalTransactions() > 0 && (weakestBucket == null
          || coverage(bucketStats.getExportedTransactions(), bucketStats.getTotalTransactions())
          < coverage(weakestBucket.getExportedTransactions(),
          weakestBucket.getTotalTransactions()))) {
        weakestBucket = bucketStats;
      }
    }

    void logSummary() {
      logger.info("Export summary | processedBuckets={} filteredBuckets={} "
              + "skippedExportedBuckets={} blocks={} emptyBlocks={} "
              + "nonEmptyBlocks={} exportedBlocks={} incompleteBlocks={} txs={} exportedTxs={} "
              + "retFallbackBlocks={} retFallbackTxs={} missingTx={} txCoverage={} "
              + "infoCoverage={} shardRecords={} shardBytes={} "
              + "load={}ms fetch={}ms process={}ms elapsed={}s",
          processedBuckets, filteredBuckets, skippedExportedBuckets, blocksFetched,
          emptyBlocks, nonEmptyBlocks,
          exportedBlocks, incompleteBlocks, totalTransactions, exportedTransactions,
          transactionRetFallbackBlocks, transactionRetFallbackTransactions, missingTransactions,
          percentage(exportedTransactions, totalTransactions),
          percentage(shardRecords, totalTransactions), shardRecords, formatBytes(shardPayloadBytes),
          shardLoadMillis, blockFetchMillis, processMillis,
          Math.max(1, (System.currentTimeMillis() - startTimeMillis) / 1000));

      if (weakestBucket != null) {
        logger.info("Export weakest bucket | range=[{}-{}] txCoverage={} infoCoverage={} "
                + "incompleteBlocks={} missingTx={} shardRecords={} totalTx={}",
            weakestBucket.getStartBlockNum(), weakestBucket.getEndBlockNum(),
            percentage(weakestBucket.getExportedTransactions(), weakestBucket.getTotalTransactions()),
            percentage(weakestBucket.getShardRecordCount(), weakestBucket.getTotalTransactions()),
            weakestBucket.getIncompleteBlocks(), weakestBucket.getMissingTransactions(),
            weakestBucket.getShardRecordCount(), weakestBucket.getTotalTransactions());
      }
    }
  }

  private static String formatBytes(long bytes) {
    if (bytes < 1024) {
      return bytes + "B";
    }
    if (bytes < 1024 * 1024) {
      return String.format("%.2fKB", bytes / 1024.0);
    }
    if (bytes < 1024L * 1024L * 1024L) {
      return String.format("%.2fMB", bytes / 1024.0 / 1024.0);
    }
    return String.format("%.2fGB", bytes / 1024.0 / 1024.0 / 1024.0);
  }

  private static String percentage(long numerator, long denominator) {
    return String.format("%.2f%%", coverage(numerator, denominator) * 100.0);
  }

  private static double coverage(long numerator, long denominator) {
    if (denominator <= 0) {
      return 0;
    }
    return (double) numerator / (double) denominator;
  }

  private static long rate(long count, long elapsedSeconds) {
    if (elapsedSeconds <= 0) {
      return 0;
    }
    return count / elapsedSeconds;
  }
}
