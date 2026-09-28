package org.tron.program;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.tron.common.application.TronApplicationContext;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.utils.JsonUtil;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.program.exporter.KafkaSender;

/**
 * Offline exporter for canonical block headers from {@code blockStore} only.
 *
 * <p>Each exported record contains exactly four JSON fields:
 * {@code triggerName}, {@code timeStamp}, {@code blockNumber}, {@code blockHash}.
 * No {@code removed} field is emitted. Blocks are validated for contiguous height and
 * parent-hash linkage on the canonical chain stored in {@code blockStore}.
 */
@Slf4j(topic = "app")
public class BlockHeaderHistoryExporter {

  private static final String BLOCK_TRIGGER_NAME = "blockTrigger";
  private static final long DEFAULT_BATCH_SIZE = 500L;
  private static final String CHECKPOINT_PROPERTY_NEXT_BLOCK = "nextExportBlockNum";
  private static final String CHECKPOINT_PROPERTY_START_BLOCK = "startBlockNum";
  private static final String CHECKPOINT_PROPERTY_END_BLOCK = "endBlockNum";
  private static final KafkaSender.ProducerConfigProfile HEADER_EXPORT_KAFKA_PROFILE =
      KafkaSender.ProducerConfigProfile.builder()
          .compressionType("lz4")
          .batchSizeBytes(524288)
          .lingerMs(50)
          .bufferMemoryBytes(134217728L)
          .maxRequestSizeBytes(1048576)
          .acks("1")
          .callbacksEnabled(true)
          .build();

  public static void main(String[] args) {
    logger.info("BlockHeaderHistoryExporter started");

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
        logger.error("Invalid block range. Start block must be >= 0, end block must be >= start "
            + "block or -1 (latest)");
        return;
      }
    } catch (NumberFormatException e) {
      logger.error("Block numbers must be valid integers", e);
      return;
    }

    String kafkaBrokers = parseStringArg(args, "-kb", null);
    String kafkaTopic = parseStringArg(args, "-kt", null);
    int kafkaRateLimit = parseIntArg(args, "-kafka-rate", 0);
    long batchSize = parseLongArg(args, "-batch", DEFAULT_BATCH_SIZE);
    String outputFile = parseStringArg(args, "-o", null);
    String checkpointFile = parseStringArg(args, "-checkpoint", null);
    boolean resume = hasFlag(args, "-resume");
    boolean resetCheckpoint = hasFlag(args, "-reset-checkpoint");

    if (batchSize <= 0) {
      batchSize = DEFAULT_BATCH_SIZE;
    }
    if ((kafkaBrokers == null) != (kafkaTopic == null)) {
      logger.error("Kafka configuration requires both -kb and -kt");
      return;
    }

    boolean useKafka = kafkaBrokers != null && kafkaTopic != null;
    if (!useKafka && (outputFile == null || outputFile.trim().isEmpty())) {
      logger.error("Specify Kafka (-kb/-kt) or a local output file (-o)");
      return;
    }

    TronApplicationContext context = null;
    KafkaSender kafkaSender = new KafkaSender();
    BufferedWriter fileWriter = null;
    ExportStats stats = new ExportStats();

    try {
      if (useKafka) {
        if (!kafkaSender.initStringProducer(kafkaBrokers, HEADER_EXPORT_KAFKA_PROFILE)) {
          logger.error("Failed to initialize Kafka producer");
          return;
        }
        if (kafkaRateLimit > 0) {
          kafkaSender.setRateLimit(kafkaRateLimit);
        }
      }

      if (outputFile != null && !outputFile.trim().isEmpty()) {
        File out = new File(outputFile);
        File parent = out.getParentFile();
        if (parent != null) {
          parent.mkdirs();
        }
        fileWriter = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(out, resume), StandardCharsets.UTF_8));
      }

      context = setupTronContext(buildConfigArgs(args));
      ChainBaseManager chainBaseManager = context.getBean(ChainBaseManager.class);

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

      long exportStartBlockNum = startBlockNum;
      if (checkpointFile != null && !checkpointFile.trim().isEmpty()) {
        Checkpoint checkpoint = loadCheckpoint(new File(checkpointFile));
        if (resetCheckpoint) {
          checkpoint = Checkpoint.create(startBlockNum, endBlockNum);
          saveCheckpoint(new File(checkpointFile), checkpoint);
          logger.info("Reset checkpoint at {}", checkpointFile);
        } else if (resume && checkpoint != null) {
          if (checkpoint.getStartBlockNum() != startBlockNum
              || checkpoint.getEndBlockNum() != endBlockNum) {
            logger.warn("Checkpoint range [{}-{}] differs from requested range [{}-{}]",
                checkpoint.getStartBlockNum(), checkpoint.getEndBlockNum(),
                startBlockNum, endBlockNum);
          }
          exportStartBlockNum = Math.max(startBlockNum, checkpoint.getNextExportBlockNum());
        } else if (checkpoint == null) {
          saveCheckpoint(new File(checkpointFile),
              Checkpoint.create(startBlockNum, endBlockNum));
        }
      }

      if (exportStartBlockNum > endBlockNum) {
        logger.info("Nothing to export. exportStart={} end={}", exportStartBlockNum, endBlockNum);
        return;
      }

      logger.info("Exporting canonical block headers for range [{}-{}], batchSize={}, kafka={}, "
              + "outputFile={}",
          exportStartBlockNum, endBlockNum, batchSize, useKafka ? kafkaTopic : "disabled",
          outputFile);

      BlockCapsule previousBlock = exportStartBlockNum > 0
          ? chainBaseManager.getBlockByNum(exportStartBlockNum - 1)
          : null;

      for (long currentStart = exportStartBlockNum; currentStart <= endBlockNum;
          currentStart += batchSize) {
        long currentEnd = Math.min(currentStart + batchSize - 1, endBlockNum);
        long limit = currentEnd - currentStart + 1;

        List<BlockCapsule> blocks = fetchBlocks(chainBaseManager, currentStart, limit);
        validateFetchedBlocks(currentStart, currentEnd, blocks);
        previousBlock = validateCanonicalChain(previousBlock, blocks);

        for (BlockCapsule block : blocks) {
          BlockHeaderRecord record = toRecord(block);
          String json = JsonUtil.obj2Json(record);
          if (json == null) {
            throw new IllegalStateException("Failed to serialize block header for block "
                + block.getNum());
          }

          if (useKafka) {
            kafkaSender.send(kafkaTopic, String.valueOf(block.getNum()), json);
          }
          if (fileWriter != null) {
            fileWriter.write(json);
            fileWriter.newLine();
          }

          stats.recordExported(block.getNum());
        }

        if (useKafka) {
          kafkaSender.flushAndVerify();
        }
        if (fileWriter != null) {
          fileWriter.flush();
        }

        if (checkpointFile != null && !checkpointFile.trim().isEmpty()) {
          saveCheckpoint(new File(checkpointFile),
              Checkpoint.create(startBlockNum, endBlockNum, currentEnd + 1));
        }

        stats.logBatchProgress(currentStart, currentEnd);
      }

      stats.logSummary(exportStartBlockNum, endBlockNum);
    } catch (Exception e) {
      logger.error("BlockHeaderHistoryExporter failed: {}", e.getMessage(), e);
      throw new RuntimeException("BlockHeaderHistoryExporter failed", e);
    } finally {
      if (fileWriter != null) {
        try {
          fileWriter.close();
        } catch (IOException e) {
          logger.warn("Failed to close output file: {}", e.getMessage());
        }
      }
      kafkaSender.close();
      if (context != null) {
        context.close();
      }
    }
  }

  private static BlockHeaderRecord toRecord(BlockCapsule block) {
    BlockHeaderRecord record = new BlockHeaderRecord();
    record.setTriggerName(BLOCK_TRIGGER_NAME);
    record.setTimeStamp(block.getTimeStamp());
    record.setBlockNumber(block.getNum());
    record.setBlockHash(block.getBlockId().toString());
    return record;
  }

  private static BlockCapsule validateCanonicalChain(BlockCapsule previousBlock,
      List<BlockCapsule> blocks) {
    BlockCapsule lastBlock = previousBlock;
    for (BlockCapsule block : blocks) {
      if (lastBlock != null && block.getNum() == lastBlock.getNum() + 1) {
        String expectedParentHash = lastBlock.getBlockId().toString();
        String actualParentHash = block.getParentHash().toString();
        if (!expectedParentHash.equals(actualParentHash)) {
          throw new IllegalStateException(String.format(
              "Non-canonical parent linkage at block %d: expectedParentHash=%s actualParentHash=%s",
              block.getNum(), expectedParentHash, actualParentHash));
        }
      }
      lastBlock = block;
    }
    return lastBlock;
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
    CommonParameter.getInstance().setMetricsPrometheusEnable(false);

    System.setProperty("database.readonly", "true");
    System.setProperty("storage.readonly", "true");

    Args.setParam(configArgs, "config.conf");

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

  private static String[] buildConfigArgs(String[] args) {
    List<String> filteredArgs = new ArrayList<>();
    for (int i = 2; i < args.length; i++) {
      if ("-batch".equals(args[i]) || "-kb".equals(args[i]) || "-kt".equals(args[i])
          || "-kafka-rate".equals(args[i]) || "-o".equals(args[i])
          || "-checkpoint".equals(args[i])) {
        i++;
      } else if ("-resume".equals(args[i]) || "-reset-checkpoint".equals(args[i])) {
        // flag only
      } else {
        filteredArgs.add(args[i]);
      }
    }
    return filteredArgs.toArray(new String[0]);
  }

  private static Checkpoint loadCheckpoint(File checkpointFile) throws IOException {
    if (!checkpointFile.exists()) {
      return null;
    }
    Properties properties = new Properties();
    try (FileInputStream inputStream = new FileInputStream(checkpointFile)) {
      properties.load(inputStream);
    }
    return new Checkpoint(
        Long.parseLong(properties.getProperty(CHECKPOINT_PROPERTY_START_BLOCK, "0")),
        Long.parseLong(properties.getProperty(CHECKPOINT_PROPERTY_END_BLOCK, "0")),
        Long.parseLong(properties.getProperty(CHECKPOINT_PROPERTY_NEXT_BLOCK, "0")));
  }

  private static void saveCheckpoint(File checkpointFile, Checkpoint checkpoint)
      throws IOException {
    File parent = checkpointFile.getParentFile();
    if (parent != null) {
      parent.mkdirs();
    }
    Properties properties = new Properties();
    properties.setProperty(CHECKPOINT_PROPERTY_START_BLOCK,
        String.valueOf(checkpoint.getStartBlockNum()));
    properties.setProperty(CHECKPOINT_PROPERTY_END_BLOCK,
        String.valueOf(checkpoint.getEndBlockNum()));
    properties.setProperty(CHECKPOINT_PROPERTY_NEXT_BLOCK,
        String.valueOf(checkpoint.getNextExportBlockNum()));
    try (FileOutputStream outputStream = new FileOutputStream(checkpointFile)) {
      properties.store(outputStream, "BlockHeaderHistoryExporter checkpoint");
    }
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
    System.out.println("  BlockHeaderHistoryExporter <startBlockNum> <endBlockNum> [options]");
    System.out.println("  BlockHeaderHistoryExporter <startBlockNum> -1 [options]");
    System.out.println("Options:");
    System.out.println("  -c <config_file>: Specify a custom configuration file");
    System.out.println("  -d <data_dir>: Specify a custom data directory");
    System.out.println("  -batch <count>: Blocks per batch, default: " + DEFAULT_BATCH_SIZE);
    System.out.println("  -kb <brokers>: Kafka broker addresses");
    System.out.println("  -kt <topic>: Kafka topic name (e.g. tron_header_backfill_v1)");
    System.out.println("  -kafka-rate <rate>: Kafka rate limit in messages/second");
    System.out.println("  -o <file>: Write one JSON record per line to a local file");
    System.out.println("  -checkpoint <file>: Checkpoint file for resume");
    System.out.println("  -resume: Continue from checkpoint nextExportBlockNum");
    System.out.println("  -reset-checkpoint: Reset checkpoint and restart from startBlockNum");
    System.out.println("Notes:");
    System.out.println("  - endBlockNum = -1 means export to the latest block in the database");
    System.out.println("  - Each record contains triggerName/timeStamp/blockNumber/blockHash only");
    System.out.println("  - Exported headers are validated for contiguous height and parent hash");
  }

  @Getter
  @Setter
  private static final class BlockHeaderRecord {

    private String triggerName;
    private long timeStamp;
    private long blockNumber;
    private String blockHash;
  }

  private static final class Checkpoint {

    private final long startBlockNum;
    private final long endBlockNum;
    private final long nextExportBlockNum;

    private Checkpoint(long startBlockNum, long endBlockNum, long nextExportBlockNum) {
      this.startBlockNum = startBlockNum;
      this.endBlockNum = endBlockNum;
      this.nextExportBlockNum = nextExportBlockNum;
    }

    private static Checkpoint create(long startBlockNum, long endBlockNum) {
      return new Checkpoint(startBlockNum, endBlockNum, startBlockNum);
    }

    private static Checkpoint create(long startBlockNum, long endBlockNum,
        long nextExportBlockNum) {
      return new Checkpoint(startBlockNum, endBlockNum, nextExportBlockNum);
    }

    private long getStartBlockNum() {
      return startBlockNum;
    }

    private long getEndBlockNum() {
      return endBlockNum;
    }

    private long getNextExportBlockNum() {
      return nextExportBlockNum;
    }
  }

  private static final class ExportStats {

    private long exportedBlocks;
    private long lastBlockNum = -1;

    private void recordExported(long blockNum) {
      exportedBlocks++;
      lastBlockNum = blockNum;
    }

    private void logBatchProgress(long batchStart, long batchEnd) {
      logger.info("Export batch [{}-{}] completed | exportedBlocks={} lastBlock={}",
          batchStart, batchEnd, exportedBlocks, lastBlockNum);
    }

    private void logSummary(long startBlockNum, long endBlockNum) {
      logger.info("Export summary | range=[{}-{}] exportedBlocks={} lastBlock={}",
          startBlockNum, endBlockNum, exportedBlocks, lastBlockNum);
    }
  }
}
