package org.tron.program;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
  private static final int DEFAULT_BLOCK_RENDER_WINDOW_SIZE = 32;
  private static final long DEFAULT_BUCKET_BLOCKS = 10_000L;
  private static final long PREPARE_PROGRESS_INTERVAL = 1_000_000L;
  private static final KafkaSender.ProducerConfigProfile SEQUENTIAL_EXPORT_STRING_KAFKA_PROFILE =
      KafkaSender.ProducerConfigProfile.builder()
          .compressionType("lz4")
          .batchSizeBytes(524288)
          .lingerMs(50)
          .bufferMemoryBytes(268435456L)
          .maxRequestSizeBytes(20 * 1024 * 1024)
          .acks("1")
          .callbacksEnabled(true)
          .build();

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
    boolean resetCheckpoint = hasFlag(args, "-reset-checkpoint");
    boolean deprecatedPreferTransactionRet = hasFlag(args, "-prefer-transaction-ret");
    boolean mergeTransactionRetRequested = hasFlag(args, "-prepare-merge-transaction-ret")
        || deprecatedPreferTransactionRet;
    boolean mergeTransactionRet = mergeTransactionRetRequested
        && (phase == Phase.PREPARE || phase == Phase.ALL);

    if (deprecatedPreferTransactionRet) {
      logger.warn("-prefer-transaction-ret is deprecated. Export now consumes shard data only. "
              + "Use -prepare-merge-transaction-ret during prepare/all to merge "
              + "transactionRetStore into shard files.");
    }
    if (phase == Phase.EXPORT && mergeTransactionRetRequested) {
      logger.warn("-prepare-merge-transaction-ret only affects prepare/all and will be ignored "
          + "for phase=export.");
    }

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
    int blockConcurrency = Math.max(DEFAULT_BLOCK_RENDER_WINDOW_SIZE,
        Math.max(8, customThreadPoolSize / 2));
    KafkaSender kafkaSender = new KafkaSender();
    TransactionProcessor processor = new TransactionProcessor(customThreadPoolSize);
    ProcessingStats stats = new ProcessingStats();
    TronApplicationContext context = null;
    File workingDir = null;
    ExportBucketManifest manifest = null;
    boolean completedSuccessfully = false;

    try {
      if (useKafka) {
        if ("trigger-proto".equals(outputFormat)) {
          if (!kafkaSender.initBytesProducer(kafkaBrokers)) {
            logger.warn("Failed to initialize Kafka proto producer. Continuing without Kafka.");
            useKafka = false;
          }
        } else if (!kafkaSender.initStringProducer(kafkaBrokers,
            SEQUENTIAL_EXPORT_STRING_KAFKA_PROFILE)) {
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

      workingDir = resolveWorkingDir(customTempDir, Args.getInstance().getOutputDirectory(),
          startBlockNum, endBlockNum);
      manifest = phase == Phase.EXPORT
          ? ExportBucketManifest.load(workingDir)
          : ExportBucketManifest.create(workingDir, startBlockNum, endBlockNum, bucketBlockCount);

      if (resetCheckpoint && phase == Phase.EXPORT) {
        logger.info("Resetting all checkpoints in manifest");
        manifest.resetCheckpoints();
        manifest.save();
      }

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
              + "effectiveExportRange=[{}-{}] bucketBlocks={} mergeTransactionRet={} tempDir={}",
          phase.name().toLowerCase(), manifest.getStartBlockNum(), manifest.getEndBlockNum(),
          startBlockNum, endBlockNum, exportStartBlockNum, exportEndBlockNum,
          manifest.getBucketBlockCount(), mergeTransactionRet,
          workingDir.getAbsolutePath());

      if (phase == Phase.PREPARE || phase == Phase.ALL) {
        PrepareStats prepareStats =
            prepareHistoryShards(chainBaseManager.getTransactionHistoryStore(), manifest);
        prepareStats.logSummary(manifest);

        if (mergeTransactionRet) {
          TransactionRetMergeStats mergeStats =
              mergeTransactionRetShards(chainBaseManager, manifest);
          mergeStats.logSummary(manifest);
        }
      }

      if (phase == Phase.EXPORT || phase == Phase.ALL) {
        ExportStats exportStats = exportBuckets(chainBaseManager, wallet, kafkaSender, processor,
            stats, manifest, exportStartBlockNum, exportEndBlockNum, outputFormat, useKafka,
            kafkaTopic, blockConcurrency);
        exportStats.logSummary();
        stats.logFinal();
      }

      completedSuccessfully = true;
    } catch (Exception e) {
      logger.error("TransactionHistorySequentialExporter failed: {}", e.getMessage(), e);
      throw new RuntimeException("TransactionHistorySequentialExporter failed", e);
    } finally {
      processor.close();
      kafkaSender.close();
      if (context != null) {
        context.close();
      }
      boolean manifestClosed = closeManifest(manifest);
      if (completedSuccessfully && workingDir != null && manifestClosed) {
        finalizeWorkingDir(phase, keepTemp, workingDir, manifest);
      } else if (completedSuccessfully && workingDir != null) {
        logger.warn("Retaining temporary shard files at {} because manifest finalization failed.",
            workingDir.getAbsolutePath());
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

  private static TransactionRetMergeStats mergeTransactionRetShards(
      ChainBaseManager chainBaseManager, ExportBucketManifest manifest) throws Exception {
    logger.info("Prepare merge phase started - sequential scan transactionRetStore");

    if (chainBaseManager.getTransactionRetStore() == null) {
      logger.warn("Skip transactionRetStore merge because transactionRetStore is unavailable");
      return new TransactionRetMergeStats();
    }

    TransactionRetMergeStats mergeStats = new TransactionRetMergeStats();
    HistoryShardLoader shardLoader = new HistoryShardLoader();
    DBIterator iterator = null;
    try {
      iterator = (DBIterator) chainBaseManager.getTransactionRetStore().getDb().iterator();
      iterator.seek(ByteArray.fromLong(manifest.getStartBlockNum()));

      for (ExportBucketManifest.Bucket bucket : manifest.getBuckets()) {
        while (iterator.valid()) {
          long blockNum = ByteArray.toLong(iterator.getKey());
          if (blockNum < bucket.getStartBlockNum()) {
            iterator.next();
            continue;
          }
          break;
        }

        if (!iterator.valid()) {
          break;
        }

        long currentBlockNum = ByteArray.toLong(iterator.getKey());
        if (currentBlockNum > manifest.getEndBlockNum()) {
          break;
        }
        if (currentBlockNum > bucket.getEndBlockNum()) {
          continue;
        }

        long bucketStartTime = System.currentTimeMillis();
        HistoryShardLoader.BucketShardData shardData =
            shardLoader.load(manifest.resolveShardFile(bucket));
        Map<Long, Map<WrappedByteArray, TransactionInfo>> bucketInfos =
            shardData.getInfosByBlock();
        boolean bucketDirty = false;
        int bucketScannedRetBlocks = 0;
        int bucketFilledBlocks = 0;
        int bucketReplacedBlocks = 0;
        int bucketConflictedBlocks = 0;
        int bucketSkippedBlocks = 0;
        long bucketMergedTransactions = 0;

        while (iterator.valid()) {
          long blockNum = ByteArray.toLong(iterator.getKey());
          if (blockNum > manifest.getEndBlockNum() || blockNum > bucket.getEndBlockNum()) {
            break;
          }

          bucketScannedRetBlocks++;
          mergeStats.recordScannedBlock();
          byte[] value = iterator.getValue();
          if (value == null) {
            bucketSkippedBlocks++;
            mergeStats.recordSkippedBlock();
            iterator.next();
            continue;
          }

          try {
            TransactionRetCapsule transactionRetCapsule = new TransactionRetCapsule(value);
            BlockMergeOutcome outcome =
                mergeTransactionRetBlock(chainBaseManager, bucketInfos, blockNum,
                    transactionRetCapsule);
            switch (outcome.getKind()) {
              case FILLED:
                bucketDirty = true;
                bucketFilledBlocks++;
                bucketMergedTransactions += outcome.getTransactionCount();
                mergeStats.recordFilledBlock(outcome.getTransactionCount());
                break;
              case REPLACED:
                bucketDirty = true;
                bucketReplacedBlocks++;
                bucketMergedTransactions += outcome.getTransactionCount();
                mergeStats.recordReplacedBlock(outcome.getTransactionCount());
                break;
              case CONFLICT_REPLACED:
                bucketDirty = true;
                bucketConflictedBlocks++;
                bucketMergedTransactions += outcome.getTransactionCount();
                mergeStats.recordConflictedBlock(outcome.getTransactionCount());
                break;
              case INVALID:
                mergeStats.recordInvalidBlock();
                break;
              case SKIPPED:
              default:
                bucketSkippedBlocks++;
                mergeStats.recordSkippedBlock();
                break;
            }
          } catch (BadItemException e) {
            mergeStats.recordMalformedBlock();
            logger.warn("Skip malformed TransactionRetCapsule during prepare merge for block {}: {}",
                blockNum, e.getMessage());
          } finally {
            iterator.next();
          }
        }

        if (!bucketDirty) {
          continue;
        }

        HistoryShardWriter.ShardStats shardStats = HistoryShardWriter.rewriteShardFile(
            manifest.resolveShardFile(bucket), bucketInfos);
        bucket.markPrepared(shardStats.getRecordCount(), shardStats.getSerializedBytes());
        mergeStats.recordRewrittenBucket();

        logger.info("Prepare merge bucket [{}-{}] | retBlocks={} filledBlocks={} "
                + "replacedBlocks={} conflictedBlocks={} skippedBlocks={} mergedTxs={} "
                + "shardRecords={} shardBytes={} elapsed={}ms",
            bucket.getStartBlockNum(), bucket.getEndBlockNum(), bucketScannedRetBlocks,
            bucketFilledBlocks, bucketReplacedBlocks, bucketConflictedBlocks,
            bucketSkippedBlocks, bucketMergedTransactions, shardStats.getRecordCount(),
            formatBytes(shardStats.getSerializedBytes()),
            System.currentTimeMillis() - bucketStartTime);
      }

      manifest.save();
    } finally {
      closeQuietly(iterator);
    }

    return mergeStats;
  }

  private static BlockMergeOutcome mergeTransactionRetBlock(
      ChainBaseManager chainBaseManager,
      Map<Long, Map<WrappedByteArray, TransactionInfo>> bucketInfos, long blockNum,
      TransactionRetCapsule transactionRetCapsule) {
    Map<WrappedByteArray, TransactionInfo> replacementInfos =
        toValidatedTransactionInfoMap(chainBaseManager, blockNum, transactionRetCapsule);
    if (replacementInfos == null) {
      return BlockMergeOutcome.invalid();
    }
    if (replacementInfos.isEmpty()) {
      return BlockMergeOutcome.skipped();
    }

    Map<WrappedByteArray, TransactionInfo> existingInfos = bucketInfos.get(blockNum);
    if (existingInfos == null || existingInfos.isEmpty()) {
      bucketInfos.put(blockNum, replacementInfos);
      return BlockMergeOutcome.filled(replacementInfos.size());
    }

    if (sameTransactionSet(existingInfos, replacementInfos)) {
      return BlockMergeOutcome.skipped();
    }

    bucketInfos.put(blockNum, replacementInfos);
    if (replacementInfos.keySet().containsAll(existingInfos.keySet())) {
      return BlockMergeOutcome.replaced(replacementInfos.size());
    }
    return BlockMergeOutcome.conflictReplaced(replacementInfos.size());
  }

  private static Map<WrappedByteArray, TransactionInfo> toValidatedTransactionInfoMap(
      ChainBaseManager chainBaseManager, long blockNum,
      TransactionRetCapsule transactionRetCapsule) {
    if (transactionRetCapsule == null || transactionRetCapsule.getInstance() == null) {
      return null;
    }

    BlockCapsule block;
    try {
      block = chainBaseManager.getBlockByNum(blockNum);
    } catch (Exception e) {
      logger.warn("Skip transactionRetStore merge for block {} because block lookup failed: {}",
          blockNum, e.getMessage());
      return null;
    }

    List<TransactionCapsule> transactions = block.getTransactions();
    List<TransactionInfo> transactionInfos =
        transactionRetCapsule.getInstance().getTransactioninfoList();
    if (transactionInfos.size() != transactions.size()) {
      logger.warn("Skip transactionRetStore merge for block {} because count mismatch: "
              + "retCount={} txCount={}", blockNum, transactionInfos.size(), transactions.size());
      return null;
    }
    if (transactionRetCapsule.getInstance().getBlockNumber() != blockNum) {
      logger.warn("Skip transactionRetStore merge for block {} because ret blockNumber={} ",
          blockNum, transactionRetCapsule.getInstance().getBlockNumber());
      return null;
    }

    Map<WrappedByteArray, TransactionInfo> indexedInfos = new java.util.HashMap<>();
    for (TransactionInfo transactionInfo : transactionInfos) {
      indexedInfos.put(WrappedByteArray.of(transactionInfo.getId().toByteArray()), transactionInfo);
    }
    if (indexedInfos.size() != transactions.size()) {
      logger.warn("Skip transactionRetStore merge for block {} because duplicate txid detected: "
              + "retCount={} uniqueRetCount={} txCount={}",
          blockNum, transactionInfos.size(), indexedInfos.size(), transactions.size());
      return null;
    }

    for (TransactionCapsule transactionCapsule : transactions) {
      WrappedByteArray txId = WrappedByteArray.of(transactionCapsule.getTransactionId().getBytes());
      if (!indexedInfos.containsKey(txId)) {
        logger.warn("Skip transactionRetStore merge for block {} because txid {} is missing in ret",
            blockNum, ByteArray.toHexString(transactionCapsule.getTransactionId().getBytes()));
        return null;
      }
    }

    return indexedInfos;
  }

  private static boolean sameTransactionSet(Map<WrappedByteArray, TransactionInfo> left,
      Map<WrappedByteArray, TransactionInfo> right) {
    return left.size() == right.size()
        && left.keySet().containsAll(right.keySet())
        && right.keySet().containsAll(left.keySet());
  }

  private static ExportStats exportBuckets(ChainBaseManager chainBaseManager, Wallet wallet,
      KafkaSender kafkaSender, TransactionProcessor processor, ProcessingStats stats,
      ExportBucketManifest manifest, long exportStartBlockNum, long exportEndBlockNum,
      String outputFormat, boolean useKafka, String kafkaTopic, int blockConcurrency)
      throws Exception {
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

      if (!manifest.supportsPrefixCheckpointResume() && bucket.hasPartialCheckpoint()) {
        throw new IllegalStateException(String.format(
            "Legacy manifest in %s contains partial checkpoint state for bucket [%d-%d] "
                + "(nextExportBlock=%d). Re-run prepare with a new -tmp directory before export.",
            manifest.getWorkingDir().getAbsolutePath(), bucket.getStartBlockNum(),
            bucket.getEndBlockNum(), bucket.getNextExportBlockNum()));
      }

      if (bucket.isExported()) {
        exportStats.recordSkippedExportedBucket();
        continue;
      }

      BucketExportStats bucketStats = new BucketExportStats(bucket);
      long requestedBucketStart = Math.max(bucket.getStartBlockNum(), exportStartBlockNum);
      long requestedBucketEnd = Math.min(bucket.getEndBlockNum(), exportEndBlockNum);
      long nextExportBlockNum = bucket.getNextExportBlockNum();
      if (requestedBucketEnd >= nextExportBlockNum && requestedBucketStart > nextExportBlockNum) {
        throw new IllegalStateException(String.format(
            "Requested export range [%d-%d] skips unexported blocks in bucket [%d-%d]. "
                + "nextExportBlock=%d. Re-run from %d or use a separate -tmp directory.",
            requestedBucketStart, requestedBucketEnd, bucket.getStartBlockNum(),
            bucket.getEndBlockNum(), nextExportBlockNum, nextExportBlockNum));
      }

      long bucketExportStart = Math.max(requestedBucketStart, nextExportBlockNum);
      long bucketExportEnd = requestedBucketEnd;
      if (bucketExportStart > bucketExportEnd) {
        exportStats.recordSkippedExportedBucket();
        continue;
      }
      logger.info("Export bucket start [{}-{}] | effectiveRange=[{}-{}] shardRecords={} "
              + "shardBytes={} exported={} nextExportBlock={}",
          bucket.getStartBlockNum(), bucket.getEndBlockNum(), bucketExportStart, bucketExportEnd,
          bucket.getRecordCount(), formatBytes(bucket.getSerializedBytes()), bucket.isExported(),
          bucket.getNextExportBlockNum());

      long loadStartTime = System.currentTimeMillis();
      HistoryShardLoader.BucketShardData shardData =
          shardLoader.load(manifest.resolveShardFile(bucket));
      long loadDurationMs = System.currentTimeMillis() - loadStartTime;
      bucketStats.recordShardLoad(shardData.getRecordCount(), shardData.getPayloadBytes(),
          loadDurationMs);

      long fetchStartTime = System.currentTimeMillis();
      List<BlockCapsule> blocks = fetchBlocks(chainBaseManager, bucketExportStart,
          bucketExportEnd - bucketExportStart + 1);
      validateFetchedBlocks(bucketExportStart, bucketExportEnd, blocks);
      long fetchDurationMs = System.currentTimeMillis() - fetchStartTime;
      bucketStats.recordBlockFetch(blocks.size(), fetchDurationMs);
      long processStartTime = System.currentTimeMillis();

      int effectiveBlockConcurrency = Math.max(1, blockConcurrency);
      for (int windowStart = 0; windowStart < blocks.size(); windowStart += effectiveBlockConcurrency) {
        int windowEnd = Math.min(windowStart + effectiveBlockConcurrency, blocks.size());
        List<CompletableFuture<BlockExportResult>> futures = new ArrayList<>(windowEnd - windowStart);

        for (int i = windowStart; i < windowEnd; i++) {
          final BlockCapsule block = blocks.get(i);
          futures.add(CompletableFuture.supplyAsync(() -> processExportBlock(block,
              shardData.getBlockInfos(block.getNum()), chainBaseManager, wallet, processor,
              outputFormat, useKafka), processor.getBlockExecutor()));
        }

        List<BlockExportResult> windowResults = new ArrayList<>(windowEnd - windowStart);
        for (CompletableFuture<BlockExportResult> future : futures) {
          try {
            windowResults.add(future.get());
          } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            logger.error("Export bucket render failure [{}-{}] | effectiveRange=[{}-{}] "
                    + "nextExportBlock={} error={}",
                bucket.getStartBlockNum(), bucket.getEndBlockNum(), bucketExportStart,
                bucketExportEnd, bucket.getNextExportBlockNum(), cause.getMessage(), cause);
            throw new IllegalStateException(String.format(
                "Render failed within bucket [%d-%d] at nextExportBlock=%d",
                bucket.getStartBlockNum(), bucket.getEndBlockNum(),
                bucket.getNextExportBlockNum()), cause);
          }
        }

        for (BlockExportResult result : windowResults) {
          if (result.isIncompleteBlock()) {
            bucketStats.recordBlockResult(result);
            logger.error("Export bucket failure [{}-{}] | block={} txCount={} missingTx={} "
                    + "incompleteBlocks={} missingTxTotal={} worstIncompleteBlock={}({} tx, "
                    + "missing={}) nextExportBlock={}",
                bucket.getStartBlockNum(), bucket.getEndBlockNum(),
                result.getBlockNum(), result.getTransactionCount(),
                result.getMissingTransactions(), bucketStats.getIncompleteBlocks(),
                bucketStats.getMissingTransactions(), bucketStats.getWorstIncompleteBlockNum(),
                bucketStats.getWorstIncompleteBlockTxCount(),
                bucketStats.getWorstIncompleteMissingTx(), bucket.getNextExportBlockNum());
            throw new IllegalStateException(String.format(
                "Incomplete export data for block %d: missingTx=%d txCount=%d. "
                    + "Prepared shard cannot fully cover this block. Re-run prepare with "
                    + "-prepare-merge-transaction-ret or rebuild shard data.",
                result.getBlockNum(), result.getMissingTransactions(),
                result.getTransactionCount()));
          }
        }

        try {
          boolean windowHasKafkaOutput = false;
          for (BlockExportResult result : windowResults) {
            if (result.hasRenderedBlock()) {
              windowHasKafkaOutput |= processor.emitRenderedBlock(result.getRenderedBlock(),
                  kafkaTopic, kafkaSender, false);
            }
          }

          if (windowHasKafkaOutput && useKafka) {
            long flushStartTime = System.nanoTime();
            kafkaSender.flushAndVerify();
            bucketStats.recordWindowKafkaFlush(System.nanoTime() - flushStartTime);
          }
        } catch (Exception e) {
          logger.error("Export bucket emit failure [{}-{}] | effectiveRange=[{}-{}] "
                  + "nextExportBlock={} error={}",
              bucket.getStartBlockNum(), bucket.getEndBlockNum(), bucketExportStart,
              bucketExportEnd, bucket.getNextExportBlockNum(), e.getMessage(), e);
          throw e;
        }

        for (BlockExportResult result : windowResults) {
          bucketStats.recordBlockResult(result);
          stats.update(1, result.getExportedTransactions(), result.getBlockNum());
        }

        long lastSuccessfulBlockNum = windowResults.get(windowResults.size() - 1).getBlockNum();
        bucket.markBlockExported(lastSuccessfulBlockNum);
        manifest.appendCheckpoint(bucket);
      }

      bucketStats.recordProcessTime(System.currentTimeMillis() - processStartTime);
      logger.info("Export bucket [{}-{}] | blocks={} txs={} shardRecords={} shardBytes={} "
              + "load={}ms fetch={}ms process={}ms emptyBlocks={} nonEmptyBlocks={} "
              + "exportedBlocks={} incompleteBlocks={} txCoverage={} infoCoverage={} missingTx={} "
              + "processDetail: index={}ms wait={}ms blockWall={}ms txWall={}ms trigger={}ms "
              + "serialize={}ms emit={}ms kafkaStr={} kafkaBytes={} prefetchHit={} "
              + "prefetchMiss={} txErrors={} slowestBlock={}({} tx, {}ms) "
              + "worstIncompleteBlock={}({} tx, missing={})",
          bucket.getStartBlockNum(), bucket.getEndBlockNum(), bucketStats.getBlocksFetched(),
          bucketStats.getTotalTransactions(), bucketStats.getShardRecordCount(),
          formatBytes(bucketStats.getShardPayloadBytes()), bucketStats.getShardLoadMillis(),
          bucketStats.getBlockFetchMillis(), bucketStats.getProcessMillis(),
          bucketStats.getEmptyBlocks(), bucketStats.getNonEmptyBlocks(),
          bucketStats.getExportedBlocks(), bucketStats.getIncompleteBlocks(),
          percentage(bucketStats.getExportedTransactions(), bucketStats.getTotalTransactions()),
          percentage(bucketStats.getShardRecordCount(), bucketStats.getTotalTransactions()),
          bucketStats.getMissingTransactions(),
          nanosToMillis(bucketStats.getIndexBuildNanos()),
          nanosToMillis(bucketStats.getFutureWaitNanos()),
          nanosToMillis(bucketStats.getBlockWallNanos()),
          nanosToMillis(bucketStats.getTransactionWallNanos()),
          nanosToMillis(bucketStats.getTriggerBuildNanos()),
          nanosToMillis(bucketStats.getSerializationNanos()),
          nanosToMillis(bucketStats.getOutputNanos()),
          bucketStats.getKafkaStringSendCount(),
          bucketStats.getKafkaBytesSendCount(),
          bucketStats.getPrefetchedTransactionInfoHitCount(),
          bucketStats.getPrefetchedTransactionInfoMissCount(),
          bucketStats.getTransactionErrorCount(),
          bucketStats.getSlowestBlockNum(), bucketStats.getSlowestBlockTxCount(),
          nanosToMillis(bucketStats.getSlowestBlockWallNanos()),
          bucketStats.getWorstIncompleteBlockNum(),
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

  private static BlockExportResult processExportBlock(BlockCapsule block,
      Map<WrappedByteArray, TransactionInfo> blockInfos, ChainBaseManager chainBaseManager,
      Wallet wallet, TransactionProcessor processor, String outputFormat, boolean useKafka) {
    List<TransactionCapsule> transactions = block.getTransactions();
    if (transactions.isEmpty()) {
      return BlockExportResult.empty(block.getNum());
    }

    PreparedBlockRet preparedBlockRet = buildPreparedTransactionRet(block, blockInfos);

    if (!preparedBlockRet.isComplete()) {
      return BlockExportResult.incomplete(block.getNum(), transactions.size(),
          preparedBlockRet.getMissingTransactionCount());
    }

    TransactionProcessor.BlockRenderResult renderedBlock =
        processor.renderTransactionsConcurrently(transactions, block.getBlockId().toString(),
            block.getNum(), block.getTimeStamp(), outputFormat, useKafka, wallet,
            chainBaseManager, preparedBlockRet.getTransactionRetCapsule());
    if (renderedBlock.getProfile().getTransactionErrorCount() > 0) {
      throw new IllegalStateException(String.format(
          "Render failed for block %d: txErrors=%d txCount=%d",
          block.getNum(), renderedBlock.getProfile().getTransactionErrorCount(),
          transactions.size()));
    }

    return BlockExportResult.rendered(block.getNum(), transactions.size(), renderedBlock);
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

  private static void validateFetchedBlocks(long expectedStartBlockNum, long expectedEndBlockNum,
      List<BlockCapsule> blocks) {
    long expectedCount = expectedEndBlockNum - expectedStartBlockNum + 1;
    if (blocks.size() != expectedCount) {
      throw new IllegalStateException(String.format(
          "Fetched block count mismatch for range [%d-%d]: expected=%d actual=%d",
          expectedStartBlockNum, expectedEndBlockNum, expectedCount, blocks.size()));
    }

    for (int i = 0; i < blocks.size(); i++) {
      BlockCapsule block = blocks.get(i);
      long expectedBlockNum = expectedStartBlockNum + i;
      if (block == null || block.getNum() != expectedBlockNum) {
        throw new IllegalStateException(String.format(
            "Fetched non-contiguous block sequence for range [%d-%d] at index=%d: "
                + "expectedBlock=%d actualBlock=%s",
            expectedStartBlockNum, expectedEndBlockNum, i, expectedBlockNum,
            block == null ? "null" : String.valueOf(block.getNum())));
      }
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
      } else if ("-keep-temp".equals(args[i]) || "-prefer-transaction-ret".equals(args[i])
          || "-prepare-merge-transaction-ret".equals(args[i]) || "-reset-checkpoint".equals(args[i])) {
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
    System.out.println("  -reset-checkpoint: Reset export checkpoints to restart from beginning");
    System.out.println("  -prepare-merge-transaction-ret: Merge transactionRetStore into shard "
        + "after scanning transactionHistoryStore");
    System.out.println("Notes:");
    System.out.println("  - endBlockNum = -1 means export to the latest block in the database");
  }

  private static void finalizeWorkingDir(Phase phase, boolean keepTemp, File workingDir,
      ExportBucketManifest manifest) {
    if (keepTemp) {
      logger.info("Temporary shard files retained at {}", workingDir.getAbsolutePath());
      return;
    }

    if (phase == Phase.PREPARE) {
      logger.info("Temporary shard files retained at {} because phase=prepare requires them "
              + "for a later export run. Use -phase export or -phase all after verification.",
          workingDir.getAbsolutePath());
      return;
    }

    if (!isManifestFullyExported(manifest)) {
      logger.info("Temporary shard files retained at {} because manifest still has "
              + "unexported buckets.",
          workingDir.getAbsolutePath());
      return;
    }

    if (deleteRecursively(workingDir)) {
      logger.info("Temporary shard files deleted from {}", workingDir.getAbsolutePath());
    } else {
      logger.warn("Failed to delete temporary shard files from {}. Remove manually if needed.",
          workingDir.getAbsolutePath());
    }
  }

  private static boolean closeManifest(ExportBucketManifest manifest) {
    if (manifest == null) {
      return true;
    }
    try {
      manifest.close();
      return true;
    } catch (Exception e) {
      logger.warn("Failed to finalize export manifest in {}: {}",
          manifest.getWorkingDir().getAbsolutePath(), e.getMessage(), e);
      return false;
    }
  }

  private static boolean deleteRecursively(File target) {
    if (target == null || !target.exists()) {
      return true;
    }

    if (target.isDirectory()) {
      File[] children = target.listFiles();
      if (children != null) {
        for (File child : children) {
          if (!deleteRecursively(child)) {
            return false;
          }
        }
      }
    }

    return target.delete();
  }

  private static boolean isManifestFullyExported(ExportBucketManifest manifest) {
    if (manifest == null) {
      return false;
    }

    for (ExportBucketManifest.Bucket bucket : manifest.getBuckets()) {
      if (!bucket.isExported()) {
        return false;
      }
    }
    return true;
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

  private static final class TransactionRetMergeStats {
    private final long startTimeMillis = System.currentTimeMillis();
    private long scannedBlocks;
    private long filledBlocks;
    private long replacedBlocks;
    private long conflictedBlocks;
    private long invalidBlocks;
    private long skippedBlocks;
    private long malformedBlocks;
    private long mergedTransactions;
    private int rewrittenBuckets;

    void recordScannedBlock() {
      scannedBlocks++;
    }

    void recordFilledBlock(int transactionCount) {
      filledBlocks++;
      mergedTransactions += transactionCount;
    }

    void recordReplacedBlock(int transactionCount) {
      replacedBlocks++;
      mergedTransactions += transactionCount;
    }

    void recordConflictedBlock(int transactionCount) {
      conflictedBlocks++;
      mergedTransactions += transactionCount;
    }

    void recordSkippedBlock() {
      skippedBlocks++;
    }

    void recordInvalidBlock() {
      invalidBlocks++;
    }

    void recordMalformedBlock() {
      malformedBlocks++;
    }

    void recordRewrittenBucket() {
      rewrittenBuckets++;
    }

    void logSummary(ExportBucketManifest manifest) {
      logger.info("Prepare merge summary | scannedRetBlocks={} filledBlocks={} "
              + "replacedBlocks={} conflictedBlocks={} invalidBlocks={} skippedBlocks={} malformedBlocks={} "
              + "rewrittenBuckets={} mergedTxs={} elapsed={}s buckets={}",
          scannedBlocks, filledBlocks, replacedBlocks, conflictedBlocks, invalidBlocks, skippedBlocks,
          malformedBlocks, rewrittenBuckets, mergedTransactions,
          Math.max(1, (System.currentTimeMillis() - startTimeMillis) / 1000),
          manifest.getBuckets().size());
    }
  }

  private static final class BlockMergeOutcome {
    private final MergeKind kind;
    private final int transactionCount;

    private BlockMergeOutcome(MergeKind kind, int transactionCount) {
      this.kind = kind;
      this.transactionCount = transactionCount;
    }

    static BlockMergeOutcome filled(int transactionCount) {
      return new BlockMergeOutcome(MergeKind.FILLED, transactionCount);
    }

    static BlockMergeOutcome replaced(int transactionCount) {
      return new BlockMergeOutcome(MergeKind.REPLACED, transactionCount);
    }

    static BlockMergeOutcome conflictReplaced(int transactionCount) {
      return new BlockMergeOutcome(MergeKind.CONFLICT_REPLACED, transactionCount);
    }

    static BlockMergeOutcome skipped() {
      return new BlockMergeOutcome(MergeKind.SKIPPED, 0);
    }

    static BlockMergeOutcome invalid() {
      return new BlockMergeOutcome(MergeKind.INVALID, 0);
    }

    MergeKind getKind() {
      return kind;
    }

    int getTransactionCount() {
      return transactionCount;
    }
  }

  private enum MergeKind {
    FILLED,
    REPLACED,
    CONFLICT_REPLACED,
    INVALID,
    SKIPPED
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
    private long totalTransactions;
    private long exportedTransactions;
    private long missingTransactions;
    private long indexBuildNanos;
    private long futureWaitNanos;
    private long blockWallNanos;
    private long transactionWallNanos;
    private long triggerBuildNanos;
    private long serializationNanos;
    private long outputNanos;
    private long kafkaStringSendCount;
    private long kafkaBytesSendCount;
    private long prefetchedTransactionInfoHitCount;
    private long prefetchedTransactionInfoMissCount;
    private long transactionErrorCount;
    private long slowestBlockNum = -1;
    private int slowestBlockTxCount;
    private long slowestBlockWallNanos;
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

    void recordProcessTime(long processMillis) {
      this.processMillis = processMillis;
    }

    void recordBlockResult(BlockExportResult result) {
      recordBlockScanned(result.getTransactionCount());
      if (result.isEmptyBlock()) {
        return;
      }

      if (result.isIncompleteBlock()) {
        recordIncompleteBlock(result.getBlockNum(), result.getTransactionCount(),
            result.getMissingTransactions());
        return;
      }

      recordExportedBlock(result.getExportedTransactions());

      TransactionProcessor.BlockProcessingProfile profile = result.getProfile();
      if (profile == null) {
        return;
      }

      indexBuildNanos += profile.getIndexBuildNanos();
      futureWaitNanos += profile.getFutureWaitNanos();
      blockWallNanos += profile.getBlockWallNanos();
      transactionWallNanos += profile.getTransactionWallNanos();
      triggerBuildNanos += profile.getTriggerBuildNanos();
      serializationNanos += profile.getSerializationNanos();
      outputNanos += profile.getOutputNanos();
      kafkaStringSendCount += profile.getKafkaStringSendCount();
      kafkaBytesSendCount += profile.getKafkaBytesSendCount();
      prefetchedTransactionInfoHitCount += profile.getPrefetchedTransactionInfoHitCount();
      prefetchedTransactionInfoMissCount += profile.getPrefetchedTransactionInfoMissCount();
      transactionErrorCount += profile.getTransactionErrorCount();

      if (profile.getBlockWallNanos() > slowestBlockWallNanos) {
        slowestBlockWallNanos = profile.getBlockWallNanos();
        slowestBlockNum = profile.getBlockNum();
        slowestBlockTxCount = profile.getTransactionCount();
      }
    }

    void recordWindowKafkaFlush(long flushNanos) {
      outputNanos += flushNanos;
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

    long getTotalTransactions() {
      return totalTransactions;
    }

    long getExportedTransactions() {
      return exportedTransactions;
    }

    long getMissingTransactions() {
      return missingTransactions;
    }

    long getIndexBuildNanos() {
      return indexBuildNanos;
    }

    long getFutureWaitNanos() {
      return futureWaitNanos;
    }

    long getBlockWallNanos() {
      return blockWallNanos;
    }

    long getTransactionWallNanos() {
      return transactionWallNanos;
    }

    long getTriggerBuildNanos() {
      return triggerBuildNanos;
    }

    long getSerializationNanos() {
      return serializationNanos;
    }

    long getOutputNanos() {
      return outputNanos;
    }

    long getKafkaStringSendCount() {
      return kafkaStringSendCount;
    }

    long getKafkaBytesSendCount() {
      return kafkaBytesSendCount;
    }

    long getPrefetchedTransactionInfoHitCount() {
      return prefetchedTransactionInfoHitCount;
    }

    long getPrefetchedTransactionInfoMissCount() {
      return prefetchedTransactionInfoMissCount;
    }

    long getTransactionErrorCount() {
      return transactionErrorCount;
    }

    long getSlowestBlockNum() {
      return slowestBlockNum;
    }

    int getSlowestBlockTxCount() {
      return slowestBlockTxCount;
    }

    long getSlowestBlockWallNanos() {
      return slowestBlockWallNanos;
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
    private long totalTransactions;
    private long exportedTransactions;
    private long missingTransactions;
    private long indexBuildNanos;
    private long futureWaitNanos;
    private long blockWallNanos;
    private long transactionWallNanos;
    private long triggerBuildNanos;
    private long serializationNanos;
    private long outputNanos;
    private long kafkaStringSendCount;
    private long kafkaBytesSendCount;
    private long prefetchedTransactionInfoHitCount;
    private long prefetchedTransactionInfoMissCount;
    private long transactionErrorCount;
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
      totalTransactions += bucketStats.getTotalTransactions();
      exportedTransactions += bucketStats.getExportedTransactions();
      missingTransactions += bucketStats.getMissingTransactions();
      indexBuildNanos += bucketStats.getIndexBuildNanos();
      futureWaitNanos += bucketStats.getFutureWaitNanos();
      blockWallNanos += bucketStats.getBlockWallNanos();
      transactionWallNanos += bucketStats.getTransactionWallNanos();
      triggerBuildNanos += bucketStats.getTriggerBuildNanos();
      serializationNanos += bucketStats.getSerializationNanos();
      outputNanos += bucketStats.getOutputNanos();
      kafkaStringSendCount += bucketStats.getKafkaStringSendCount();
      kafkaBytesSendCount += bucketStats.getKafkaBytesSendCount();
      prefetchedTransactionInfoHitCount += bucketStats.getPrefetchedTransactionInfoHitCount();
      prefetchedTransactionInfoMissCount += bucketStats.getPrefetchedTransactionInfoMissCount();
      transactionErrorCount += bucketStats.getTransactionErrorCount();

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
              + "missingTx={} txCoverage={} infoCoverage={} shardRecords={} shardBytes={} "
              + "load={}ms fetch={}ms process={}ms "
              + "processDetail: index={}ms wait={}ms blockWall={}ms txWall={}ms trigger={}ms "
              + "serialize={}ms emit={}ms kafkaStr={} kafkaBytes={} prefetchHit={} "
              + "prefetchMiss={} txErrors={} elapsed={}s",
          processedBuckets, filteredBuckets, skippedExportedBuckets, blocksFetched,
          emptyBlocks, nonEmptyBlocks,
          exportedBlocks, incompleteBlocks, totalTransactions, exportedTransactions,
          missingTransactions, percentage(exportedTransactions, totalTransactions),
          percentage(shardRecords, totalTransactions), shardRecords, formatBytes(shardPayloadBytes),
          shardLoadMillis, blockFetchMillis, processMillis,
          nanosToMillis(indexBuildNanos), nanosToMillis(futureWaitNanos),
          nanosToMillis(blockWallNanos), nanosToMillis(transactionWallNanos),
          nanosToMillis(triggerBuildNanos), nanosToMillis(serializationNanos),
          nanosToMillis(outputNanos), kafkaStringSendCount, kafkaBytesSendCount,
          prefetchedTransactionInfoHitCount, prefetchedTransactionInfoMissCount,
          transactionErrorCount,
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

  private static final class BlockExportResult {
    private final long blockNum;
    private final int transactionCount;
    private final int missingTransactions;
    private final boolean emptyBlock;
    private final boolean incompleteBlock;
    private final TransactionProcessor.BlockRenderResult renderedBlock;

    private BlockExportResult(long blockNum, int transactionCount, int missingTransactions,
        boolean emptyBlock, boolean incompleteBlock,
        TransactionProcessor.BlockRenderResult renderedBlock) {
      this.blockNum = blockNum;
      this.transactionCount = transactionCount;
      this.missingTransactions = missingTransactions;
      this.emptyBlock = emptyBlock;
      this.incompleteBlock = incompleteBlock;
      this.renderedBlock = renderedBlock;
    }

    static BlockExportResult empty(long blockNum) {
      return new BlockExportResult(blockNum, 0, 0, true, false, null);
    }

    static BlockExportResult incomplete(long blockNum, int transactionCount,
        int missingTransactions) {
      return new BlockExportResult(blockNum, transactionCount, missingTransactions,
          false, true, null);
    }

    static BlockExportResult rendered(long blockNum, int transactionCount,
        TransactionProcessor.BlockRenderResult renderedBlock) {
      return new BlockExportResult(blockNum, transactionCount, 0,
          false, false, renderedBlock);
    }

    long getBlockNum() {
      return blockNum;
    }

    int getTransactionCount() {
      return transactionCount;
    }

    int getExportedTransactions() {
      if (!hasRenderedBlock() || getProfile() == null) {
        return 0;
      }
      return getProfile().getSuccessfulOutputTransactionCount();
    }

    int getMissingTransactions() {
      return missingTransactions;
    }

    boolean isEmptyBlock() {
      return emptyBlock;
    }

    boolean isIncompleteBlock() {
      return incompleteBlock;
    }

    boolean hasRenderedBlock() {
      return renderedBlock != null;
    }

    TransactionProcessor.BlockRenderResult getRenderedBlock() {
      return renderedBlock;
    }

    TransactionProcessor.BlockProcessingProfile getProfile() {
      return renderedBlock == null ? null : renderedBlock.getProfile();
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

  private static long nanosToMillis(long nanos) {
    return nanos / 1_000_000L;
  }
}
