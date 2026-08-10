package org.tron.program;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.BooleanUtils;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.tron.common.application.TronApplicationContext;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.utils.ByteArray;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.capsule.TransactionInfoCapsule;
import org.tron.core.capsule.TransactionRetCapsule;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.store.TransactionHistoryStore;
import org.tron.core.store.TransactionRetStore;
import org.tron.protos.Protocol.TransactionInfo;

/**
 * Offline backfill tool for reconstructing TransactionRetStore from TransactionHistoryStore.
 *
 * <p>Supports:
 * <ul>
 *   <li>scan — scan coverage and estimate recoverable blocks</li>
 *   <li>backfill — write missing TransactionRetCapsule into transactionRetStore</li>
 *   <li>verify — verify block tx ids against existing transactionRetStore entries</li>
 * </ul>
 */
@Slf4j(topic = "app")
public class TransactionRetBackfiller {

  private static final long DEFAULT_BATCH_SIZE = 200;

  public static void main(String[] args) {
    logger.info("TransactionRetBackfiller started");

    Range range;
    try {
      range = parseRange(args);
    } catch (IllegalArgumentException e) {
      logger.error(e.getMessage());
      printUsage();
      return;
    }

    Mode mode;
    try {
      mode = Mode.from(parseStringArg(args, "-mode", "backfill"));
    } catch (IllegalArgumentException e) {
      logger.error(e.getMessage());
      printUsage();
      return;
    }

    long batchSize = parseLongArg(args, "-batch", DEFAULT_BATCH_SIZE);
    if (batchSize <= 0) {
      batchSize = DEFAULT_BATCH_SIZE;
    }
    boolean overwriteExisting = hasFlag(args, "-overwrite-existing");

    TronApplicationContext context = null;
    try {
      String[] configArgs = buildConfigArgs(args);
      context = setupTronContext(configArgs);

      ChainBaseManager chainBaseManager = context.getBean(ChainBaseManager.class);
      TransactionRetStore transactionRetStore = chainBaseManager.getTransactionRetStore();
      TransactionHistoryStore transactionHistoryStore = chainBaseManager.getTransactionHistoryStore();

      ensureWritableConfig();

      long lowestBlockNum = getLowestBlockNum(chainBaseManager);
      long latestBlockNum = getLatestBlockNum(chainBaseManager);

      long startBlockNum = range.specified ? range.startBlockNum : lowestBlockNum;
      long endBlockNum = range.specified ? range.endBlockNum : latestBlockNum;
      if (endBlockNum == -1) {
        endBlockNum = latestBlockNum;
      }
      if (startBlockNum < lowestBlockNum || endBlockNum > latestBlockNum) {
        logger.error("Requested block range [{}-{}] is outside database range [{}-{}]",
            startBlockNum, endBlockNum, lowestBlockNum, latestBlockNum);
        return;
      }

      logger.info("Mode={}, overwriteExisting={}, batchSize={}", mode.name().toLowerCase(),
          overwriteExisting, batchSize);
      logger.info("Database block range: {} to {}", lowestBlockNum, latestBlockNum);
      logger.info("Target block range: {} to {}", startBlockNum, endBlockNum);

      BackfillStats stats = new BackfillStats(mode, startBlockNum, endBlockNum);

      for (long currentStart = startBlockNum; currentStart <= endBlockNum; currentStart += batchSize) {
        long currentEnd = Math.min(currentStart + batchSize - 1, endBlockNum);
        List<BlockCapsule> blocks = fetchBlocks(chainBaseManager, currentStart,
            currentEnd - currentStart + 1);
        if (blocks.isEmpty()) {
          logger.warn("No blocks fetched for range [{}-{}]", currentStart, currentEnd);
          continue;
        }

        for (BlockCapsule block : blocks) {
          switch (mode) {
            case SCAN:
              scanBlock(block, transactionRetStore, transactionHistoryStore,
                  overwriteExisting, stats);
              break;
            case BACKFILL:
              backfillBlock(block, transactionRetStore, transactionHistoryStore,
                  overwriteExisting, stats);
              break;
            case VERIFY:
              verifyBlock(block, transactionRetStore, stats);
              break;
            default:
              throw new IllegalStateException("Unsupported mode: " + mode);
          }
        }

        stats.logProgress(currentStart, currentEnd);
      }

      stats.logSummary();
    } catch (Exception e) {
      logger.error("TransactionRetBackfiller failed: {}", e.getMessage(), e);
    } finally {
      if (context != null) {
        context.close();
      }
    }
  }

  private static void scanBlock(BlockCapsule block, TransactionRetStore transactionRetStore,
      TransactionHistoryStore transactionHistoryStore, boolean overwriteExisting,
      BackfillStats stats) {
    stats.incrementBlocksScanned();

    List<TransactionCapsule> transactions = block.getTransactions();
    if (transactions.isEmpty()) {
      stats.incrementEmptyBlocks();
      return;
    }

    stats.incrementNonEmptyBlocks();
    byte[] blockKey = ByteArray.fromLong(block.getNum());
    TransactionRetCapsule existingRet = readTransactionRet(transactionRetStore, blockKey);
    if (existingRet != null && existingRet.getInstance() != null) {
      stats.incrementExistingRetBlocks();
      if (!overwriteExisting) {
        return;
      }
    } else {
      stats.incrementMissingRetBlocks();
    }

    BuildResult buildResult = buildTransactionRet(block, transactionHistoryStore);
    if (!buildResult.isComplete()) {
      stats.incrementIncompleteBlocks();
      stats.addMissingTransactions(buildResult.getMissingTransactionCount());
      logger.warn("Scan incomplete block={} missingTx={} txCount={}",
          block.getNum(), buildResult.getMissingTransactionCount(), transactions.size());
      return;
    }

    stats.incrementBackfillableBlocks();
    stats.addEstimatedBytes(buildResult.getSerializedSize());
  }

  private static void backfillBlock(BlockCapsule block, TransactionRetStore transactionRetStore,
      TransactionHistoryStore transactionHistoryStore, boolean overwriteExisting,
      BackfillStats stats) {
    stats.incrementBlocksScanned();

    List<TransactionCapsule> transactions = block.getTransactions();
    if (transactions.isEmpty()) {
      stats.incrementEmptyBlocks();
      return;
    }

    stats.incrementNonEmptyBlocks();
    byte[] blockKey = ByteArray.fromLong(block.getNum());
    TransactionRetCapsule existingRet = readTransactionRet(transactionRetStore, blockKey);
    if (existingRet != null && existingRet.getInstance() != null) {
      stats.incrementExistingRetBlocks();
      if (!overwriteExisting) {
        return;
      }
    } else {
      stats.incrementMissingRetBlocks();
    }

    BuildResult buildResult = buildTransactionRet(block, transactionHistoryStore);
    if (!buildResult.isComplete()) {
      stats.incrementIncompleteBlocks();
      stats.addMissingTransactions(buildResult.getMissingTransactionCount());
      logger.warn("Backfill incomplete block={} missingTx={} txCount={}",
          block.getNum(), buildResult.getMissingTransactionCount(), transactions.size());
      return;
    }

    transactionRetStore.put(blockKey, buildResult.getTransactionRetCapsule());
    stats.incrementWrittenBlocks();
    stats.addEstimatedBytes(buildResult.getSerializedSize());
  }

  private static void verifyBlock(BlockCapsule block, TransactionRetStore transactionRetStore,
      BackfillStats stats) {
    stats.incrementBlocksScanned();

    List<TransactionCapsule> transactions = block.getTransactions();
    if (transactions.isEmpty()) {
      stats.incrementEmptyBlocks();
      return;
    }

    stats.incrementNonEmptyBlocks();
    TransactionRetCapsule transactionRetCapsule = readTransactionRet(transactionRetStore,
        ByteArray.fromLong(block.getNum()));
    if (transactionRetCapsule == null || transactionRetCapsule.getInstance() == null) {
      stats.incrementMissingRetBlocks();
      logger.warn("Verify missing block={} txCount={}", block.getNum(), transactions.size());
      return;
    }

    List<TransactionInfo> infos = transactionRetCapsule.getInstance().getTransactioninfoList();
    if (infos.size() != transactions.size()) {
      stats.incrementVerificationFailures();
      logger.warn("Verify count mismatch block={} retCount={} txCount={}",
          block.getNum(), infos.size(), transactions.size());
      return;
    }

    if (transactionRetCapsule.getInstance().getBlockNumber() != block.getNum()) {
      stats.incrementVerificationFailures();
      logger.warn("Verify blockNumber mismatch block={} retBlockNumber={}",
          block.getNum(), transactionRetCapsule.getInstance().getBlockNumber());
      return;
    }

    Set<WrappedByteArray> txIds = new HashSet<>(transactions.size());
    for (TransactionCapsule transactionCapsule : transactions) {
      txIds.add(WrappedByteArray.of(transactionCapsule.getTransactionId().getBytes()));
    }

    for (TransactionInfo info : infos) {
      if (!txIds.contains(WrappedByteArray.of(info.getId().toByteArray()))) {
        stats.incrementVerificationFailures();
        logger.warn("Verify txid mismatch block={} txid={}",
            block.getNum(), ByteArray.toHexString(info.getId().toByteArray()));
        return;
      }
    }

    stats.incrementVerifiedBlocks();
  }

  private static BuildResult buildTransactionRet(BlockCapsule block,
      TransactionHistoryStore transactionHistoryStore) {
    List<TransactionCapsule> transactions = block.getTransactions();
    List<byte[]> txIds = new ArrayList<>(transactions.size());
    for (TransactionCapsule transactionCapsule : transactions) {
      txIds.add(transactionCapsule.getTransactionId().getBytes());
    }

    Map<WrappedByteArray, TransactionInfoCapsule> infoMap = transactionHistoryStore
        .getUnchecked(txIds);
    List<TransactionInfo> orderedInfos = new ArrayList<>(transactions.size());
    int missingTransactionCount = 0;

    for (TransactionCapsule transactionCapsule : transactions) {
      WrappedByteArray txId = WrappedByteArray.of(transactionCapsule.getTransactionId().getBytes());
      TransactionInfoCapsule infoCapsule = infoMap.get(txId);
      if (infoCapsule == null || infoCapsule.getInstance() == null) {
        missingTransactionCount++;
        continue;
      }
      orderedInfos.add(infoCapsule.getInstance());
    }

    if (missingTransactionCount > 0) {
      return BuildResult.incomplete(missingTransactionCount);
    }

    TransactionRetCapsule transactionRetCapsule = new TransactionRetCapsule(block);
    transactionRetCapsule.addAllTransactionInfos(orderedInfos);
    return BuildResult.complete(transactionRetCapsule);
  }

  private static TransactionRetCapsule readTransactionRet(TransactionRetStore transactionRetStore,
      byte[] blockKey) {
    try {
      return transactionRetStore.getTransactionInfoByBlockNum(blockKey);
    } catch (Exception e) {
      logger.warn("Failed to read TransactionRetCapsule for block {}: {}",
          ByteArray.toLong(blockKey), e.getMessage());
      return null;
    }
  }

  private static void ensureWritableConfig() {
    if (!BooleanUtils.toBoolean(
        CommonParameter.getInstance().getStorage().getTransactionHistorySwitch())) {
      throw new IllegalStateException("storage.transHistory.switch must be enabled for backfill");
    }
  }

  private static TronApplicationContext setupTronContext(String[] configArgs) {
    CommonParameter parameter = CommonParameter.getInstance();
    parameter.setNeedToUpdateAsset(false);
    parameter.setP2pDisable(true);
    parameter.setRpcEnable(false);
    parameter.setRpcSolidityEnable(false);
    parameter.setRpcPBFTEnable(false);
    parameter.setFullNodeHttpEnable(false);
    parameter.setSolidityNodeHttpEnable(false);
    parameter.setPBFTHttpEnable(false);
    parameter.setJsonRpcHttpFullNodeEnable(false);
    parameter.setJsonRpcHttpSolidityNodeEnable(false);
    parameter.setJsonRpcHttpPBFTNodeEnable(false);
    parameter.setEventSubscribe(false);
    parameter.setNodeMetricsEnable(false);
    parameter.setMetricsPrometheusEnable(false);

    Args.setParam(configArgs, "config.conf");

    String databasePath = Args.getInstance().getOutputDirectory();
    logger.info("Database directory: {}", databasePath);
    File dbDir = new File(databasePath);
    if (!dbDir.exists()) {
      throw new IllegalStateException("Database directory does not exist: " + databasePath);
    }

    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    beanFactory.setAllowCircularReferences(false);
    TronApplicationContext context = new TronApplicationContext(beanFactory);
    context.register(DefaultConfig.class);
    context.refresh();
    logger.info("Writable database context initialized successfully");
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
          // Ignore, fall through to lowest.
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

  private static String parseStringArg(String[] args, String flag, String defaultValue) {
    for (int i = 0; i < args.length - 1; i++) {
      if (flag.equals(args[i])) {
        return args[i + 1];
      }
    }
    return defaultValue;
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

  private static String[] buildConfigArgs(String[] args) {
    List<String> filteredArgs = new ArrayList<>();
    int startIndex = hasLeadingRangeArgs(args) ? 2 : 0;
    for (int i = startIndex; i < args.length; i++) {
      if ("-mode".equals(args[i]) || "-batch".equals(args[i])) {
        i++;
      } else if ("-overwrite-existing".equals(args[i])) {
        // flag only
      } else {
        filteredArgs.add(args[i]);
      }
    }
    return filteredArgs.toArray(new String[0]);
  }

  private static void printUsage() {
    System.out.println("Usage:");
    System.out.println("  TransactionRetBackfiller [startBlockNum endBlockNum] [options]");
    System.out.println("Options:");
    System.out.println("  -c <config_file>: Specify a custom configuration file");
    System.out.println("  -d <data_dir>: Specify a custom data directory");
    System.out.println("  -mode <scan|backfill|verify>: Operation mode, default: backfill");
    System.out.println("  -batch <count>: Batch size, default: " + DEFAULT_BATCH_SIZE);
    System.out.println("  -overwrite-existing: Rebuild blocks even if transactionRetStore already exists");
    System.out.println("Notes:");
    System.out.println("  - If no block range is provided, the whole database range is processed");
    System.out.println("  - This tool is intended for offline execution on a stopped node or DB copy");
  }

  private static Range parseRange(String[] args) {
    if (args.length > 0 && !args[0].startsWith("-") && !hasLeadingRangeArgs(args)) {
      throw new IllegalArgumentException(
          "Block range must provide both startBlockNum and endBlockNum");
    }

    if (!hasLeadingRangeArgs(args)) {
      return Range.full();
    }

    try {
      long startBlockNum = Long.parseLong(args[0]);
      long endBlockNum = Long.parseLong(args[1]);
      if (startBlockNum < 0 || (endBlockNum < 0 && endBlockNum != -1)
          || (endBlockNum >= 0 && startBlockNum > endBlockNum)) {
        throw new IllegalArgumentException("Invalid block range. Start block must be >= 0, "
            + "end block must be >= start block or -1");
      }
      return Range.of(startBlockNum, endBlockNum);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Block numbers must be valid integers", e);
    }
  }

  private static boolean hasLeadingRangeArgs(String[] args) {
    return args.length >= 2 && !args[0].startsWith("-") && !args[1].startsWith("-");
  }

  private enum Mode {
    SCAN,
    BACKFILL,
    VERIFY;

    static Mode from(String value) {
      if (value == null) {
        return SCAN;
      }
      switch (value.toLowerCase()) {
        case "scan":
          return SCAN;
        case "backfill":
          return BACKFILL;
        case "verify":
          return VERIFY;
        default:
          throw new IllegalArgumentException("Unsupported mode: " + value);
      }
    }
  }

  private static final class BuildResult {
    private final TransactionRetCapsule transactionRetCapsule;
    private final int missingTransactionCount;
    private final int serializedSize;

    private BuildResult(TransactionRetCapsule transactionRetCapsule, int missingTransactionCount,
        int serializedSize) {
      this.transactionRetCapsule = transactionRetCapsule;
      this.missingTransactionCount = missingTransactionCount;
      this.serializedSize = serializedSize;
    }

    static BuildResult complete(TransactionRetCapsule transactionRetCapsule) {
      byte[] data = transactionRetCapsule.getData();
      return new BuildResult(transactionRetCapsule, 0, data == null ? 0 : data.length);
    }

    static BuildResult incomplete(int missingTransactionCount) {
      return new BuildResult(null, missingTransactionCount, 0);
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

    int getSerializedSize() {
      return serializedSize;
    }
  }

  private static final class BackfillStats {
    private final Mode mode;
    private final long startBlockNum;
    private final long endBlockNum;
    private final long startTimeMillis = System.currentTimeMillis();

    private long blocksScanned;
    private long emptyBlocks;
    private long nonEmptyBlocks;
    private long existingRetBlocks;
    private long missingRetBlocks;
    private long backfillableBlocks;
    private long incompleteBlocks;
    private long writtenBlocks;
    private long verifiedBlocks;
    private long verificationFailures;
    private long missingTransactions;
    private long estimatedBytes;

    private BackfillStats(Mode mode, long startBlockNum, long endBlockNum) {
      this.mode = mode;
      this.startBlockNum = startBlockNum;
      this.endBlockNum = endBlockNum;
    }

    void incrementBlocksScanned() {
      blocksScanned++;
    }

    void incrementEmptyBlocks() {
      emptyBlocks++;
    }

    void incrementNonEmptyBlocks() {
      nonEmptyBlocks++;
    }

    void incrementExistingRetBlocks() {
      existingRetBlocks++;
    }

    void incrementMissingRetBlocks() {
      missingRetBlocks++;
    }

    void incrementBackfillableBlocks() {
      backfillableBlocks++;
    }

    void incrementIncompleteBlocks() {
      incompleteBlocks++;
    }

    void incrementWrittenBlocks() {
      writtenBlocks++;
    }

    void incrementVerifiedBlocks() {
      verifiedBlocks++;
    }

    void incrementVerificationFailures() {
      verificationFailures++;
    }

    void addMissingTransactions(long count) {
      missingTransactions += count;
    }

    void addEstimatedBytes(long bytes) {
      estimatedBytes += bytes;
    }

    void logProgress(long currentStart, long currentEnd) {
      logger.info("Mode={} progress [{}-{}] | scanned={} nonEmpty={} existingRet={} "
              + "missingRet={} backfillable={} incomplete={} written={} verified={} verifyFail={} "
              + "missingTx={} estimatedSize={}MB elapsed={}s",
          mode.name().toLowerCase(), currentStart, currentEnd,
          blocksScanned, nonEmptyBlocks, existingRetBlocks, missingRetBlocks,
          backfillableBlocks, incompleteBlocks, writtenBlocks, verifiedBlocks,
          verificationFailures, missingTransactions,
          estimatedBytes / 1024 / 1024,
          (System.currentTimeMillis() - startTimeMillis) / 1000);
      }

    void logSummary() {
      logger.info(toReportString());
    }

    String toReportString() {
      return String.format("summary mode=%s range=%d-%d scanned=%d empty=%d nonEmpty=%d "
              + "existingRet=%d missingRet=%d backfillable=%d incomplete=%d written=%d "
              + "verified=%d verifyFail=%d missingTx=%d estimatedBytes=%d elapsed=%ds",
          mode.name().toLowerCase(), startBlockNum, endBlockNum, blocksScanned, emptyBlocks,
          nonEmptyBlocks, existingRetBlocks, missingRetBlocks, backfillableBlocks,
          incompleteBlocks, writtenBlocks, verifiedBlocks, verificationFailures,
          missingTransactions, estimatedBytes,
          (System.currentTimeMillis() - startTimeMillis) / 1000);
    }
  }

  private static final class Range {
    private final boolean specified;
    private final long startBlockNum;
    private final long endBlockNum;

    private Range(boolean specified, long startBlockNum, long endBlockNum) {
      this.specified = specified;
      this.startBlockNum = startBlockNum;
      this.endBlockNum = endBlockNum;
    }

    static Range full() {
      return new Range(false, 0, -1);
    }

    static Range of(long startBlockNum, long endBlockNum) {
      return new Range(true, startBlockNum, endBlockNum);
    }
  }
}
