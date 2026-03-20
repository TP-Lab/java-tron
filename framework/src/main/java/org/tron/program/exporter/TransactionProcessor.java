package org.tron.program.exporter;

import com.google.protobuf.ByteString;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.tron.common.logsfilter.trigger.TransactionLogTrigger;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.JsonUtil;
import org.tron.core.ChainBaseManager;
import org.tron.core.Wallet;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.capsule.TransactionInfoCapsule;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.services.http.JsonFormat;
import org.tron.core.services.http.Util;
import org.tron.protos.Protocol.Transaction;
import org.tron.protos.Protocol.TransactionInfo;
import org.tron.protos.Protocol.TransactionInfo.Log;
import org.tron.protos.TransactionLogTriggerProtos;

/**
 * 交易处理器 — 从 BlockTransactionPrinter 中提取的交易处理逻辑。
 *
 * <p>职责：
 * <ul>
 *   <li>管理双层线程池（block-level + transaction-level）</li>
 *   <li>批量预取 TransactionInfo 并发处理交易</li>
 *   <li>根据 outputFormat 分发到不同输出路径（trigger / trigger-proto / json / protobuf）</li>
 * </ul>
 *
 * <p>关键依赖通过方法参数注入：
 * <ul>
 *   <li>{@link KafkaSender} — Kafka 发送</li>
 *   <li>{@link TriggerBuilder} — Trigger 构建</li>
 *   <li>{@link TriggerProtoConverter} — Protobuf 转换</li>
 * </ul>
 */
@Slf4j(topic = "app")
public class TransactionProcessor implements Closeable {

  private final ExecutorService blockExecutor;
  private final ExecutorService transactionExecutor;

  /**
   * 构造函数 — 初始化双层线程池。
   *
   * @param threadPoolSize transaction-level 线程池大小；
   *                       block-level 线程池大小为 max(8, threadPoolSize / 2)
   */
  public TransactionProcessor(int threadPoolSize) {
    int blockPoolSize = Math.max(8, threadPoolSize / 2);
    this.blockExecutor = Executors.newFixedThreadPool(blockPoolSize);
    logger.info("Block processing thread pool initialized with {} threads", blockPoolSize);

    this.transactionExecutor = Executors.newFixedThreadPool(threadPoolSize);
    logger.info("Transaction processing thread pool initialized with {} threads", threadPoolSize);
  }

  // ========================================================================
  // 公共 API
  // ========================================================================

  public ExecutorService getBlockExecutor() {
    return blockExecutor;
  }

  public ExecutorService getTransactionExecutor() {
    return transactionExecutor;
  }

  /**
   * 并发处理一个区块内的所有交易（核心入口）。
   *
   * <p>优化策略：
   * <ol>
   *   <li>批量预取 TransactionInfo — 将 N 次随机 I/O 转换为 1 次顺序 I/O</li>
   *   <li>构建内存 HashMap — 后续 O(1) 查找，零 I/O</li>
   *   <li>并发提交到 transactionExecutor — 使用预取数据，零 I/O</li>
   * </ol>
   */
  public BlockProcessingProfile processTransactionsConcurrently(
      List<TransactionCapsule> transactions,
      String blockId, long blockNum, long timestamp,
      String outputFormat, boolean useKafka, String kafkaTopic,
      Wallet wallet, ChainBaseManager chainBaseManager,
      KafkaSender kafkaSender) {
    return processTransactionsConcurrently(transactions, blockId, blockNum, timestamp,
        outputFormat, useKafka, kafkaTopic, wallet, chainBaseManager, kafkaSender, null);
  }

  public BlockProcessingProfile processTransactionsConcurrently(
      List<TransactionCapsule> transactions,
      String blockId, long blockNum, long timestamp,
      String outputFormat, boolean useKafka, String kafkaTopic,
      Wallet wallet, ChainBaseManager chainBaseManager,
      KafkaSender kafkaSender,
      org.tron.core.capsule.TransactionRetCapsule prefetchedTransactionRet) {
    long blockStartTime = System.nanoTime();
    BlockProcessingProfile profile = new BlockProcessingProfile(blockNum, transactions.size());

    if (transactions.isEmpty()) {
      profile.addBlockWallNanos(System.nanoTime() - blockStartTime);
      return profile;
    }

    logger.debug("Processing {} transactions concurrently...", transactions.size());

    // ====================================================================
    // 第一步：构建 TransactionInfo 索引（优先使用外部预读数据，零 I/O）
    // ====================================================================
    long indexBuildStartTime = System.nanoTime();
    Map<String, TransactionInfo> transactionInfoMap = new HashMap<>();
    org.tron.core.capsule.TransactionRetCapsule transactionRetCapsule = prefetchedTransactionRet;
    profile.setUsedPrefetchedTransactionRet(prefetchedTransactionRet != null);

    // 如果外部没有预读，则自行读取（降级）
    if (transactionRetCapsule == null) {
      try {
        if (chainBaseManager != null && chainBaseManager.getTransactionRetStore() != null) {
          long fallbackRetStartTime = System.nanoTime();
          byte[] blockNumKey = ByteArray.fromLong(blockNum);
          transactionRetCapsule =
              chainBaseManager.getTransactionRetStore().getTransactionInfoByBlockNum(blockNumKey);
          profile.recordTransactionRetFallbackRead(System.nanoTime() - fallbackRetStartTime,
              transactionRetCapsule != null);
          logger.debug("Fallback: read TransactionRetCapsule for block {}", blockNum);
        }
      } catch (Exception e) {
        profile.incrementTransactionErrors();
        logger.warn("Failed to read TransactionRetCapsule for block {}: {}", blockNum,
            e.getMessage());
      }
    }

    if (transactionRetCapsule != null && transactionRetCapsule.getInstance() != null) {
      for (TransactionInfo info : transactionRetCapsule.getInstance().getTransactioninfoList()) {
        String txId = ByteArray.toHexString(info.getId().toByteArray());
        transactionInfoMap.put(txId, info);
      }
      logger.debug("Indexed {} TransactionInfo for block {}", transactionInfoMap.size(), blockNum);
    } else {
      logger.debug("No TransactionRetCapsule for block {}, trying TransactionHistoryStore",
          blockNum);
      if (chainBaseManager != null && chainBaseManager.getTransactionHistoryStore() != null) {
        long historyPrefetchStartTime = System.nanoTime();
        List<byte[]> transactionIds = new ArrayList<>(transactions.size());
        for (TransactionCapsule transactionCapsule : transactions) {
          transactionIds.add(transactionCapsule.getTransactionId().getBytes());
        }
        Map<WrappedByteArray, TransactionInfoCapsule> transactionInfoCapsules = chainBaseManager
            .getTransactionHistoryStore()
            .getUnchecked(transactionIds);
        int historyPrefetchCount = 0;
        for (TransactionInfoCapsule transactionInfoCapsule : transactionInfoCapsules.values()) {
          if (transactionInfoCapsule != null && transactionInfoCapsule.getInstance() != null) {
            TransactionInfo info = transactionInfoCapsule.getInstance();
            String txId = ByteArray.toHexString(info.getId().toByteArray());
            transactionInfoMap.put(txId, info);
            historyPrefetchCount++;
          }
        }
        profile.recordTransactionHistoryPrefetch(
            System.nanoTime() - historyPrefetchStartTime, historyPrefetchCount);
        logger.debug("Indexed {} TransactionInfo from TransactionHistoryStore for block {}",
            historyPrefetchCount, blockNum);
      }
    }
    profile.setIndexedTransactionInfoCount(transactionInfoMap.size());
    profile.addIndexBuildNanos(System.nanoTime() - indexBuildStartTime);

    // ====================================================================
    // 第二步：并发处理每个交易（使用预取的数据）
    // ====================================================================
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    AtomicInteger processedCount = new AtomicInteger(0);
    final int txCount = transactions.size();

    for (int i = 0; i < txCount; i++) {
      final TransactionCapsule trx = transactions.get(i);
      final String txId = trx.getTransactionId().toString();
      final int transactionIndex = i;
      final TransactionInfo prefetchedTransactionInfo = transactionInfoMap.get(txId);
      if (prefetchedTransactionInfo != null) {
        profile.incrementPrefetchedTransactionInfoHitCount();
      } else {
        profile.incrementPrefetchedTransactionInfoMissCount();
      }

      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        try {
          processTransactionWithPrefetchedInfo(trx, prefetchedTransactionInfo, transactionIndex,
              blockId, blockNum, timestamp, outputFormat, useKafka, kafkaTopic, wallet,
              kafkaSender, profile);

          int completed = processedCount.incrementAndGet();
          if (completed % 50 == 0 || completed == transactions.size()) {
            logger.debug("Processed {}/{} transactions in block {}",
                completed, transactions.size(), blockNum);
          }
        } catch (Exception e) {
          profile.incrementTransactionErrors();
          logger.error("Error processing transaction {} in block {}: {}",
              transactionIndex + 1, blockNum, e.getMessage(), e);
        }
      }, transactionExecutor);

      futures.add(future);
    }

    // 等待所有交易处理完成
    try {
      long waitStartTime = System.nanoTime();
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
      profile.addFutureWaitNanos(System.nanoTime() - waitStartTime);
      logger.debug("All {} transactions processed successfully in block {}",
          transactions.size(), blockNum);
    } catch (Exception e) {
      profile.incrementTransactionErrors();
      logger.error("Error waiting for concurrent transaction processing to complete: {}",
          e.getMessage(), e);
    }

    profile.addBlockWallNanos(System.nanoTime() - blockStartTime);
    return profile;
  }

  /**
   * 使用预取的 TransactionInfo 处理单个交易（优化路径）。
   *
   * <p>降级逻辑：
   * <ul>
   *   <li>如果 prefetchedTransactionInfo 不为 null → 零 I/O</li>
   *   <li>如果 prefetchedTransactionInfo 为 null → 自动降级为 wallet 单个查询</li>
   *   <li>如果 TransactionCapsule.getInstance() 失败 → 降级为 wallet.getTransactionById()</li>
   * </ul>
   */
  public void processTransactionWithPrefetchedInfo(
      TransactionCapsule trx, TransactionInfo prefetchedTransactionInfo,
      int transactionIndex, String blockId, long blockNum, long timestamp,
      String outputFormat, boolean useKafka, String kafkaTopic, Wallet wallet,
      KafkaSender kafkaSender, BlockProcessingProfile profile) {
    long transactionStartTime = System.nanoTime();

    String txId = trx.getTransactionId().toString();
    logger.debug("Processing transaction #{} (ID: {}) in block {}",
        transactionIndex + 1, txId, blockNum);

    // 第一步：获取 TransactionInfo — 优先使用预取数据（零 I/O）
    TransactionInfo transactionInfo = prefetchedTransactionInfo;
    if (transactionInfo == null) {
      long txInfoFallbackStartTime = System.nanoTime();
      ByteString txIdBytes = ByteString.copyFrom(ByteArray.fromHexString(txId));
      transactionInfo = wallet.getTransactionInfoById(txIdBytes);
      profile.recordTransactionInfoFallbackQuery(System.nanoTime() - txInfoFallbackStartTime,
          transactionInfo != null);
      logger.debug("Using fallback query for TransactionInfo (prefetch missed): {}", txId);
    }
    if (transactionInfo == null) {
      profile.incrementMissingTransactionInfoCount();
    }

    // 第二步：获取 Transaction — 直接从 TransactionCapsule 获取（零 I/O）
    Transaction transaction = null;
    try {
      long transactionCapsuleReadStartTime = System.nanoTime();
      transaction = trx.getInstance();
      profile.addTransactionCapsuleReadNanos(System.nanoTime() - transactionCapsuleReadStartTime);
      logger.debug("Retrieved transaction directly from TransactionCapsule for ID: {}", txId);
    } catch (Exception e) {
      logger.warn("Could not get transaction from TransactionCapsule for ID {}: {}",
          txId, e.getMessage());
      // 降级处理：仅在 TransactionCapsule 失败时才查询数据库
      long transactionFallbackStartTime = System.nanoTime();
      ByteString txIdBytes = ByteString.copyFrom(ByteArray.fromHexString(txId));
      transaction = wallet.getTransactionById(txIdBytes);
      profile.recordTransactionFallbackQuery(System.nanoTime() - transactionFallbackStartTime,
          transaction != null);
    }
    if (transaction == null) {
      profile.incrementMissingTransactionCount();
    }

    // 第三步：处理交易数据
    processTransactionData(transactionInfo, transaction, trx, transactionIndex,
        blockId, blockNum, timestamp, outputFormat, useKafka, kafkaTopic, kafkaSender, profile);
    profile.addTransactionWallNanos(System.nanoTime() - transactionStartTime);
  }

  /**
   * 遗留方法 — 不使用预取，自动降级为单个查询。
   */
  public void processTransaction(
      TransactionCapsule trx, int transactionIndex,
      String blockId, long blockNum, long timestamp,
      String outputFormat, boolean useKafka, String kafkaTopic, Wallet wallet,
      KafkaSender kafkaSender) {

    processTransactionWithPrefetchedInfo(trx, null, transactionIndex,
        blockId, blockNum, timestamp, outputFormat, useKafka, kafkaTopic, wallet, kafkaSender,
        new BlockProcessingProfile(blockNum, 1));
  }

  /**
   * 根据 outputFormat 分发到不同输出路径。
   *
   * <p>消除了原 BlockTransactionPrinter 中 printTransactionLogTrigger 的 4 个重载，
   * 在此方法中直接处理 trigger 格式的输出逻辑。
   *
   * <p>支持的格式：
   * <ul>
   *   <li>trigger — 调用 TriggerBuilder 构建 POJO，JsonUtil 序列化为 JSON，可选 Kafka 发送</li>
   *   <li>trigger-proto — 调用 TriggerBuilder 构建 POJO，TriggerProtoConverter 转为 protobuf，
   *       kafkaSender.sendBytes() 发送</li>
   *   <li>json / protobuf / both — 传统格式输出到 stdout</li>
   * </ul>
   */
  public void processTransactionData(
      TransactionInfo transactionInfo, Transaction transaction,
      TransactionCapsule trx, int transactionIndex,
      String blockId, long blockNum, long timestamp,
      String outputFormat, boolean useKafka, String kafkaTopic,
      KafkaSender kafkaSender, BlockProcessingProfile profile) {

    String txId = trx.getTransactionId().toString();

    if ("trigger".equals(outputFormat) || "trigger-proto".equals(outputFormat)) {
      String kafkaTopicToUse = useKafka ? kafkaTopic : null;
      String kafkaKey = txId;

      if (transactionInfo == null && transaction == null) {
        logger.warn("Missing transaction data for ID {} in block {} "
            + "- this may be normal for genesis block transactions", txId, blockNum);
      }

      if ("trigger-proto".equals(outputFormat)) {
        // ---- trigger-proto: Protobuf binary 输出 ----
        try {
          long triggerBuildStartTime = System.nanoTime();
          TransactionLogTrigger trigger = TriggerBuilder.createTransactionLogTrigger(
              transactionInfo, transaction, blockId, blockNum, timestamp,
              transactionIndex, trx);
          profile.addTriggerBuildNanos(System.nanoTime() - triggerBuildStartTime);

          long serializeStartTime = System.nanoTime();
          TransactionLogTriggerProtos.TransactionLogTriggerPB protoMsg =
              TriggerProtoConverter.convert(trigger);
          byte[] payload = protoMsg.toByteArray();
          profile.addSerializationNanos(System.nanoTime() - serializeStartTime);
          profile.incrementTriggerProtoCount();

          long emitStartTime = System.nanoTime();
          if (kafkaTopicToUse != null && kafkaSender != null
              && kafkaSender.hasBytesProducer()) {
            String key = kafkaKey != null ? kafkaKey : trigger.getTransactionId();
            if (isValidTransactionId(trigger.getTransactionId())) {
              kafkaSender.sendBytes(kafkaTopicToUse, key, payload);
              profile.incrementKafkaBytesSendCount();
            } else {
              profile.incrementInvalidTransactionIdSkipCount();
              logger.warn("Skipped Kafka proto send due to invalid transaction ID: {}",
                  trigger.getTransactionId());
            }
          } else {
            // 没有 Kafka 时输出 base64 到控制台
            System.out.println(Base64.getEncoder().encodeToString(payload));
            profile.incrementStdoutOutputCount();
          }
          profile.addOutputNanos(System.nanoTime() - emitStartTime);
        } catch (Exception e) {
          profile.incrementTransactionErrors();
          logger.error("Error creating protobuf TransactionLogTrigger: " + e.getMessage(), e);
        }
      } else {
        // ---- trigger: JSON 输出 ----
        try {
          long triggerBuildStartTime = System.nanoTime();
          TransactionLogTrigger trigger = TriggerBuilder.createTransactionLogTrigger(
              transactionInfo, transaction, blockId, blockNum, timestamp,
              transactionIndex, trx);
          profile.addTriggerBuildNanos(System.nanoTime() - triggerBuildStartTime);

          long serializeStartTime = System.nanoTime();
          String jsonOutput = JsonUtil.obj2Json(trigger);
          profile.addSerializationNanos(System.nanoTime() - serializeStartTime);
          profile.incrementTriggerJsonCount();

          if (jsonOutput != null) {
            boolean sentToKafka = false;
            long emitStartTime = System.nanoTime();

            if (kafkaTopicToUse != null && kafkaSender != null
                && kafkaSender.hasStringProducer()) {
              String key = kafkaKey != null ? kafkaKey : trigger.getTransactionId();
              if (isValidTransactionId(trigger.getTransactionId())) {
                kafkaSender.send(kafkaTopicToUse, key, jsonOutput);
                profile.incrementKafkaStringSendCount();
                sentToKafka = true;
              } else {
                profile.incrementInvalidTransactionIdSkipCount();
                logger.warn("Skipped Kafka send due to invalid transaction ID: {}",
                    trigger.getTransactionId());
              }
            }

            // 未发送到 Kafka 时输出到控制台
            if (!sentToKafka) {
              System.out.println(jsonOutput);
              profile.incrementStdoutOutputCount();
            }
            profile.addOutputNanos(System.nanoTime() - emitStartTime);
          } else {
            profile.incrementSerializationNullCount();
            logger.error("Failed to serialize TransactionLogTrigger to JSON for transaction");
          }
        } catch (Exception e) {
          profile.incrementTransactionErrors();
          logger.error("Error creating TransactionLogTrigger: " + e.getMessage(), e);
        }
      }
    } else {
      // ---- 传统格式（json / protobuf / both）----
      long emitStartTime = System.nanoTime();
      synchronized (System.out) {
        if (transactionInfo != null) {
          List<Log> newLogList = Util.convertLogAddressToTronAddress(transactionInfo);
          TransactionInfo transactionInfoWithConvertedLogs = transactionInfo.toBuilder()
              .clearLog()
              .addAllLog(newLogList)
              .build();

          if ("json".equals(outputFormat) || "both".equals(outputFormat)) {
            System.out.println(
                JsonFormat.printToString(transactionInfoWithConvertedLogs, true));
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
      profile.addOutputNanos(System.nanoTime() - emitStartTime);
      profile.incrementLegacyOutputCount();
    }
  }

  // ========================================================================
  // Closeable — 优雅关闭双层线程池
  // ========================================================================

  @Override
  public void close() {
    shutdownExecutor("block", blockExecutor);
    shutdownExecutor("transaction", transactionExecutor);
  }

  // ========================================================================
  // 内部工具方法
  // ========================================================================

  private static boolean isValidTransactionId(String txId) {
    return txId != null
        && !"N/A".equals(txId)
        && !txId.isEmpty()
        && !txId.startsWith("UNKNOWN_TX_");
  }

  public static final class BlockProcessingProfile {

    private final long blockNum;
    private final int transactionCount;
    private volatile boolean usedPrefetchedTransactionRet;
    private final LongAdder blockWallNanos = new LongAdder();
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
    private final AtomicInteger indexedTransactionInfoCount = new AtomicInteger();
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

    public BlockProcessingProfile(long blockNum, int transactionCount) {
      this.blockNum = blockNum;
      this.transactionCount = transactionCount;
    }

    public long getBlockNum() {
      return blockNum;
    }

    public int getTransactionCount() {
      return transactionCount;
    }

    public boolean isUsedPrefetchedTransactionRet() {
      return usedPrefetchedTransactionRet;
    }

    public void setUsedPrefetchedTransactionRet(boolean usedPrefetchedTransactionRet) {
      this.usedPrefetchedTransactionRet = usedPrefetchedTransactionRet;
    }

    public void addBlockWallNanos(long nanos) {
      blockWallNanos.add(nanos);
    }

    public long getBlockWallNanos() {
      return blockWallNanos.sum();
    }

    public void addIndexBuildNanos(long nanos) {
      indexBuildNanos.add(nanos);
    }

    public long getIndexBuildNanos() {
      return indexBuildNanos.sum();
    }

    public void addFutureWaitNanos(long nanos) {
      futureWaitNanos.add(nanos);
    }

    public long getFutureWaitNanos() {
      return futureWaitNanos.sum();
    }

    public void addTransactionWallNanos(long nanos) {
      transactionWallNanos.add(nanos);
    }

    public long getTransactionWallNanos() {
      return transactionWallNanos.sum();
    }

    public void addTransactionCapsuleReadNanos(long nanos) {
      transactionCapsuleReadNanos.add(nanos);
    }

    public long getTransactionCapsuleReadNanos() {
      return transactionCapsuleReadNanos.sum();
    }

    public void recordTransactionInfoFallbackQuery(long nanos, boolean hit) {
      transactionInfoFallbackQueryNanos.add(nanos);
      transactionInfoFallbackQueryCount.incrementAndGet();
      if (hit) {
        transactionInfoFallbackHitCount.incrementAndGet();
      }
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

    public void recordTransactionFallbackQuery(long nanos, boolean hit) {
      transactionFallbackQueryNanos.add(nanos);
      transactionFallbackQueryCount.incrementAndGet();
      if (hit) {
        transactionFallbackHitCount.incrementAndGet();
      }
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

    public void recordTransactionRetFallbackRead(long nanos, boolean hit) {
      transactionRetFallbackReadNanos.add(nanos);
      transactionRetFallbackReadCount.incrementAndGet();
      if (hit) {
        transactionRetFallbackHitCount.incrementAndGet();
      }
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

    public void recordTransactionHistoryPrefetch(long nanos, int infoCount) {
      transactionHistoryPrefetchNanos.add(nanos);
      transactionHistoryPrefetchCount.incrementAndGet();
      transactionHistoryPrefetchInfoCount.addAndGet(infoCount);
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

    public void addTriggerBuildNanos(long nanos) {
      triggerBuildNanos.add(nanos);
    }

    public long getTriggerBuildNanos() {
      return triggerBuildNanos.sum();
    }

    public void addSerializationNanos(long nanos) {
      serializationNanos.add(nanos);
    }

    public long getSerializationNanos() {
      return serializationNanos.sum();
    }

    public void addOutputNanos(long nanos) {
      outputNanos.add(nanos);
    }

    public long getOutputNanos() {
      return outputNanos.sum();
    }

    public void setIndexedTransactionInfoCount(int count) {
      indexedTransactionInfoCount.set(count);
    }

    public int getIndexedTransactionInfoCount() {
      return indexedTransactionInfoCount.get();
    }

    public void incrementPrefetchedTransactionInfoHitCount() {
      prefetchedTransactionInfoHitCount.incrementAndGet();
    }

    public int getPrefetchedTransactionInfoHitCount() {
      return prefetchedTransactionInfoHitCount.get();
    }

    public void incrementPrefetchedTransactionInfoMissCount() {
      prefetchedTransactionInfoMissCount.incrementAndGet();
    }

    public int getPrefetchedTransactionInfoMissCount() {
      return prefetchedTransactionInfoMissCount.get();
    }

    public void incrementMissingTransactionInfoCount() {
      missingTransactionInfoCount.incrementAndGet();
    }

    public int getMissingTransactionInfoCount() {
      return missingTransactionInfoCount.get();
    }

    public void incrementMissingTransactionCount() {
      missingTransactionCount.incrementAndGet();
    }

    public int getMissingTransactionCount() {
      return missingTransactionCount.get();
    }

    public void incrementTransactionErrors() {
      transactionErrorCount.incrementAndGet();
    }

    public int getTransactionErrorCount() {
      return transactionErrorCount.get();
    }

    public void incrementTriggerJsonCount() {
      triggerJsonCount.incrementAndGet();
    }

    public int getTriggerJsonCount() {
      return triggerJsonCount.get();
    }

    public void incrementTriggerProtoCount() {
      triggerProtoCount.incrementAndGet();
    }

    public int getTriggerProtoCount() {
      return triggerProtoCount.get();
    }

    public void incrementLegacyOutputCount() {
      legacyOutputCount.incrementAndGet();
    }

    public int getLegacyOutputCount() {
      return legacyOutputCount.get();
    }

    public void incrementKafkaStringSendCount() {
      kafkaStringSendCount.incrementAndGet();
    }

    public int getKafkaStringSendCount() {
      return kafkaStringSendCount.get();
    }

    public void incrementKafkaBytesSendCount() {
      kafkaBytesSendCount.incrementAndGet();
    }

    public int getKafkaBytesSendCount() {
      return kafkaBytesSendCount.get();
    }

    public void incrementStdoutOutputCount() {
      stdoutOutputCount.incrementAndGet();
    }

    public int getStdoutOutputCount() {
      return stdoutOutputCount.get();
    }

    public void incrementInvalidTransactionIdSkipCount() {
      invalidTransactionIdSkipCount.incrementAndGet();
    }

    public int getInvalidTransactionIdSkipCount() {
      return invalidTransactionIdSkipCount.get();
    }

    public void incrementSerializationNullCount() {
      serializationNullCount.incrementAndGet();
    }

    public int getSerializationNullCount() {
      return serializationNullCount.get();
    }
  }

  private static void shutdownExecutor(String name, ExecutorService executor) {
    if (executor == null) {
      return;
    }
    try {
      logger.debug("Shutting down {} processing thread pool...", name);
      executor.shutdown();
      if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
        logger.warn("{} thread pool did not terminate gracefully, forcing shutdown...", name);
        executor.shutdownNow();
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
          logger.error("{} thread pool did not terminate after forced shutdown", name);
        }
      }
      logger.info("{} processing thread pool shut down successfully", name);
    } catch (InterruptedException e) {
      logger.error("{} thread pool shutdown interrupted", name, e);
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
