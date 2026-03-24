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

  private static final int DEFAULT_TRANSACTION_CHUNK_SIZE = 128;

  private final ExecutorService blockExecutor;
  private final ExecutorService transactionExecutor;
  private volatile int transactionChunkSize = DEFAULT_TRANSACTION_CHUNK_SIZE;

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
    BlockRenderResult renderedBlock = renderTransactionsConcurrently(transactions, blockId,
        blockNum, timestamp, outputFormat, useKafka, wallet, chainBaseManager,
        prefetchedTransactionRet);
    emitRenderedBlock(renderedBlock, kafkaTopic, kafkaSender);
    return renderedBlock.getProfile();
  }

  public BlockRenderResult renderTransactionsConcurrently(
      List<TransactionCapsule> transactions,
      String blockId, long blockNum, long timestamp,
      String outputFormat, boolean useKafka,
      Wallet wallet, ChainBaseManager chainBaseManager,
      org.tron.core.capsule.TransactionRetCapsule prefetchedTransactionRet) {
    long blockStartTime = System.nanoTime();
    BlockProcessingProfile profile = new BlockProcessingProfile(blockNum, transactions.size());

    if (transactions.isEmpty()) {
      profile.addBlockWallNanos(System.nanoTime() - blockStartTime);
      return new BlockRenderResult(blockNum, 0, new ArrayList<TransactionOutputRecord>(), profile);
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
    AtomicInteger processedCount = new AtomicInteger(0);
    final int txCount = transactions.size();
    int effectiveChunkSize = Math.max(1, Math.min(transactionChunkSize, txCount));
    List<TransactionOutputRecord> outputRecords = new ArrayList<>();

    // 等待所有交易处理完成
    if (txCount <= effectiveChunkSize) {
      outputRecords.addAll(renderTransactionChunk(transactions, 0, txCount, transactionInfoMap,
          blockId, blockNum, timestamp, outputFormat, useKafka, wallet, profile,
          processedCount));
    } else {
      List<CompletableFuture<List<TransactionOutputRecord>>> futures = new ArrayList<>();
      for (int chunkStart = 0; chunkStart < txCount; chunkStart += effectiveChunkSize) {
        final int fromIndex = chunkStart;
        final int toIndex = Math.min(chunkStart + effectiveChunkSize, txCount);
        CompletableFuture<List<TransactionOutputRecord>> future = CompletableFuture.supplyAsync(() ->
            renderTransactionChunk(transactions, fromIndex, toIndex, transactionInfoMap,
                blockId, blockNum, timestamp, outputFormat, useKafka, wallet, profile,
                processedCount), transactionExecutor);
        futures.add(future);
      }

      try {
        long waitStartTime = System.nanoTime();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        profile.addFutureWaitNanos(System.nanoTime() - waitStartTime);
        for (CompletableFuture<List<TransactionOutputRecord>> future : futures) {
          outputRecords.addAll(future.get());
        }
        logger.debug("All {} transactions processed successfully in block {}",
            transactions.size(), blockNum);
      } catch (Exception e) {
        profile.incrementTransactionErrors();
        logger.error("Error waiting for concurrent transaction processing to complete: {}",
            e.getMessage(), e);
      }
    }

    profile.addBlockWallNanos(System.nanoTime() - blockStartTime);
    return new BlockRenderResult(blockNum, transactions.size(), outputRecords, profile);
  }

  public void emitRenderedBlock(BlockRenderResult renderedBlock, String kafkaTopic,
      KafkaSender kafkaSender) {
    emitRenderedBlock(renderedBlock, kafkaTopic, kafkaSender, true);
  }

  public boolean emitRenderedBlock(BlockRenderResult renderedBlock, String kafkaTopic,
      KafkaSender kafkaSender, boolean verifyAfterEmit) {
    return emitOutputRecords(renderedBlock.getOutputRecords(), kafkaTopic, kafkaSender,
        renderedBlock.getProfile(), verifyAfterEmit);
  }

  private List<TransactionOutputRecord> renderTransactionChunk(List<TransactionCapsule> transactions,
      int fromIndex,
      int toIndex, Map<String, TransactionInfo> transactionInfoMap, String blockId, long blockNum,
      long timestamp, String outputFormat, boolean useKafka, Wallet wallet,
      BlockProcessingProfile profile, AtomicInteger processedCount) {
    List<TransactionOutputRecord> outputRecords =
        new ArrayList<>(Math.max(1, toIndex - fromIndex));
    for (int i = fromIndex; i < toIndex; i++) {
      TransactionCapsule trx = transactions.get(i);
      String txId = trx.getTransactionId().toString();
      TransactionInfo prefetchedTransactionInfo = transactionInfoMap.get(txId);
      if (prefetchedTransactionInfo != null) {
        profile.incrementPrefetchedTransactionInfoHitCount();
      } else {
        profile.incrementPrefetchedTransactionInfoMissCount();
      }
      try {
        outputRecords.addAll(renderTransactionWithPrefetchedInfo(trx, prefetchedTransactionInfo, i,
            blockId, blockNum, timestamp, outputFormat, useKafka, wallet, profile));

        int completed = processedCount.incrementAndGet();
        if (completed % 50 == 0 || completed == transactions.size()) {
          logger.debug("Processed {}/{} transactions in block {}",
              completed, transactions.size(), blockNum);
        }
      } catch (Exception e) {
        profile.incrementTransactionErrors();
        logger.error("Error processing transaction {} in block {}: {}",
            i + 1, blockNum, e.getMessage(), e);
      }
    }
    return outputRecords;
  }

  private List<TransactionOutputRecord> renderTransactionWithPrefetchedInfo(
      TransactionCapsule trx, TransactionInfo prefetchedTransactionInfo,
      int transactionIndex, String blockId, long blockNum, long timestamp,
      String outputFormat, boolean useKafka, Wallet wallet,
      BlockProcessingProfile profile) {
    long transactionStartTime = System.nanoTime();

    String txId = trx.getTransactionId().toString();
    logger.debug("Rendering transaction #{} (ID: {}) in block {}",
        transactionIndex + 1, txId, blockNum);

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

    Transaction transaction = null;
    try {
      long transactionCapsuleReadStartTime = System.nanoTime();
      transaction = trx.getInstance();
      profile.addTransactionCapsuleReadNanos(System.nanoTime() - transactionCapsuleReadStartTime);
      logger.debug("Retrieved transaction directly from TransactionCapsule for ID: {}", txId);
    } catch (Exception e) {
      logger.warn("Could not get transaction from TransactionCapsule for ID {}: {}",
          txId, e.getMessage());
      long transactionFallbackStartTime = System.nanoTime();
      ByteString txIdBytes = ByteString.copyFrom(ByteArray.fromHexString(txId));
      transaction = wallet.getTransactionById(txIdBytes);
      profile.recordTransactionFallbackQuery(System.nanoTime() - transactionFallbackStartTime,
          transaction != null);
    }
    if (transaction == null) {
      profile.incrementMissingTransactionCount();
    }

    List<TransactionOutputRecord> outputRecords = renderTransactionData(transactionInfo, transaction,
        trx, transactionIndex, blockId, blockNum, timestamp, outputFormat, useKafka, profile);
    if (hasExpectedTransactionOutput(outputRecords, useKafka)) {
      profile.incrementSuccessfulOutputTransactionCount();
    } else {
      profile.incrementTransactionErrors();
      logger.error("No valid output produced for transaction {} in block {}. format={} "
              + "useKafka={} outputRecords={} invalidTxIdSkips={} serializationNulls={}",
          txId, blockNum, outputFormat, useKafka, outputRecords.size(),
          profile.getInvalidTransactionIdSkipCount(), profile.getSerializationNullCount());
    }
    profile.addTransactionWallNanos(System.nanoTime() - transactionStartTime);
    return outputRecords;
  }

  private boolean emitOutputRecords(List<TransactionOutputRecord> outputRecords, String kafkaTopic,
      KafkaSender kafkaSender, BlockProcessingProfile profile, boolean verifyAfterEmit) {
    long emitStartTime = System.nanoTime();
    List<String> stdoutLines = new ArrayList<>();
    boolean kafkaOutputPresent = false;

    for (TransactionOutputRecord outputRecord : outputRecords) {
      switch (outputRecord.getType()) {
        case KAFKA_STRING:
          if (kafkaSender == null || kafkaTopic == null || !kafkaSender.hasStringProducer()) {
            throw new IllegalStateException("Kafka string producer is not available");
          }
          kafkaSender.send(kafkaTopic, outputRecord.getKey(), outputRecord.getTextPayload());
          profile.incrementKafkaStringSendCount();
          kafkaOutputPresent = true;
          break;
        case KAFKA_BYTES:
          if (kafkaSender == null || kafkaTopic == null || !kafkaSender.hasBytesProducer()) {
            throw new IllegalStateException("Kafka bytes producer is not available");
          }
          kafkaSender.sendBytes(kafkaTopic, outputRecord.getKey(), outputRecord.getBinaryPayload());
          profile.incrementKafkaBytesSendCount();
          kafkaOutputPresent = true;
          break;
        case STDOUT:
          stdoutLines.add(outputRecord.getTextPayload());
          break;
        default:
          throw new IllegalStateException("Unsupported output type: " + outputRecord.getType());
      }
    }

    if (!stdoutLines.isEmpty()) {
      synchronized (System.out) {
        for (String stdoutLine : stdoutLines) {
          System.out.println(stdoutLine);
          profile.incrementStdoutOutputCount();
        }
      }
    }

    if (verifyAfterEmit && kafkaOutputPresent && kafkaSender != null) {
      kafkaSender.flushAndVerify();
    }

    long emitDuration = System.nanoTime() - emitStartTime;
    profile.addOutputNanos(emitDuration);
    profile.addBlockWallNanos(emitDuration);
    return kafkaOutputPresent;
  }

  private boolean hasExpectedTransactionOutput(List<TransactionOutputRecord> outputRecords,
      boolean useKafka) {
    if (outputRecords == null || outputRecords.isEmpty()) {
      return false;
    }

    if (!useKafka) {
      return true;
    }

    for (TransactionOutputRecord outputRecord : outputRecords) {
      if (outputRecord.getType() == OutputType.KAFKA_STRING
          || outputRecord.getType() == OutputType.KAFKA_BYTES) {
        return true;
      }
    }
    return false;
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
    List<TransactionOutputRecord> outputRecords = renderTransactionWithPrefetchedInfo(trx,
        prefetchedTransactionInfo, transactionIndex, blockId, blockNum, timestamp, outputFormat,
        useKafka, wallet, profile);
    emitOutputRecords(outputRecords, kafkaTopic, kafkaSender, profile, true);
  }

  /**
   * 遗留方法 — 不使用预取，自动降级为单个查询。
   */
  @Deprecated
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
    List<TransactionOutputRecord> outputRecords = renderTransactionData(transactionInfo,
        transaction, trx, transactionIndex, blockId, blockNum, timestamp, outputFormat,
        useKafka, profile);
    emitOutputRecords(outputRecords, kafkaTopic, kafkaSender, profile, true);
  }

  private List<TransactionOutputRecord> renderTransactionData(
      TransactionInfo transactionInfo, Transaction transaction,
      TransactionCapsule trx, int transactionIndex,
      String blockId, long blockNum, long timestamp,
      String outputFormat, boolean useKafka, BlockProcessingProfile profile) {
    List<TransactionOutputRecord> outputRecords = new ArrayList<>(2);
    String txId = trx.getTransactionId().toString();

    if ("trigger".equals(outputFormat) || "trigger-proto".equals(outputFormat)) {
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

          if (useKafka) {
            String key = kafkaKey != null ? kafkaKey : trigger.getTransactionId();
            if (isValidTransactionId(trigger.getTransactionId())) {
              outputRecords.add(TransactionOutputRecord.kafkaBytes(key, payload));
            } else {
              profile.incrementInvalidTransactionIdSkipCount();
              logger.warn("Skipped Kafka proto send due to invalid transaction ID: {}",
                  trigger.getTransactionId());
            }
          } else {
            outputRecords.add(TransactionOutputRecord.stdout(
                Base64.getEncoder().encodeToString(payload)));
          }
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
            if (useKafka) {
              String key = kafkaKey != null ? kafkaKey : trigger.getTransactionId();
              if (isValidTransactionId(trigger.getTransactionId())) {
                outputRecords.add(TransactionOutputRecord.kafkaString(key, jsonOutput));
              } else {
                profile.incrementInvalidTransactionIdSkipCount();
                logger.warn("Skipped Kafka send due to invalid transaction ID: {}",
                    trigger.getTransactionId());
                outputRecords.add(TransactionOutputRecord.stdout(jsonOutput));
              }
            } else {
              outputRecords.add(TransactionOutputRecord.stdout(jsonOutput));
            }
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
      if (transactionInfo != null) {
        List<Log> newLogList = Util.convertLogAddressToTronAddress(transactionInfo);
        TransactionInfo transactionInfoWithConvertedLogs = transactionInfo.toBuilder()
            .clearLog()
            .addAllLog(newLogList)
            .build();

        if ("json".equals(outputFormat) || "both".equals(outputFormat)) {
          outputRecords.add(TransactionOutputRecord.stdout(
              JsonFormat.printToString(transactionInfoWithConvertedLogs, true)));
        }
        if ("protobuf".equals(outputFormat) || "both".equals(outputFormat)) {
          outputRecords.add(TransactionOutputRecord.stdout(
              transactionInfoWithConvertedLogs.toString()));
        }
      }

      if (transaction != null) {
        if ("json".equals(outputFormat) || "both".equals(outputFormat)) {
          outputRecords.add(TransactionOutputRecord.stdout(
              JsonFormat.printToString(transaction, true)));
        }
        if ("protobuf".equals(outputFormat) || "both".equals(outputFormat)) {
          outputRecords.add(TransactionOutputRecord.stdout(transaction.toString()));
        }
      }
      profile.incrementLegacyOutputCount();
    }
    return outputRecords;
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

  public static final class BlockRenderResult {
    private final long blockNum;
    private final int transactionCount;
    private final List<TransactionOutputRecord> outputRecords;
    private final BlockProcessingProfile profile;

    private BlockRenderResult(long blockNum, int transactionCount,
        List<TransactionOutputRecord> outputRecords, BlockProcessingProfile profile) {
      this.blockNum = blockNum;
      this.transactionCount = transactionCount;
      this.outputRecords = outputRecords;
      this.profile = profile;
    }

    public long getBlockNum() {
      return blockNum;
    }

    public int getTransactionCount() {
      return transactionCount;
    }

    List<TransactionOutputRecord> getOutputRecords() {
      return outputRecords;
    }

    public BlockProcessingProfile getProfile() {
      return profile;
    }
  }

  private static final class TransactionOutputRecord {
    private final OutputType type;
    private final String key;
    private final String textPayload;
    private final byte[] binaryPayload;

    private TransactionOutputRecord(OutputType type, String key, String textPayload,
        byte[] binaryPayload) {
      this.type = type;
      this.key = key;
      this.textPayload = textPayload;
      this.binaryPayload = binaryPayload;
    }

    static TransactionOutputRecord kafkaString(String key, String textPayload) {
      return new TransactionOutputRecord(OutputType.KAFKA_STRING, key, textPayload, null);
    }

    static TransactionOutputRecord kafkaBytes(String key, byte[] binaryPayload) {
      return new TransactionOutputRecord(OutputType.KAFKA_BYTES, key, null, binaryPayload);
    }

    static TransactionOutputRecord stdout(String textPayload) {
      return new TransactionOutputRecord(OutputType.STDOUT, null, textPayload, null);
    }

    OutputType getType() {
      return type;
    }

    String getKey() {
      return key;
    }

    String getTextPayload() {
      return textPayload;
    }

    byte[] getBinaryPayload() {
      return binaryPayload;
    }
  }

  private enum OutputType {
    KAFKA_STRING,
    KAFKA_BYTES,
    STDOUT
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
    private final AtomicInteger successfulOutputTransactionCount = new AtomicInteger();

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

    public void incrementSuccessfulOutputTransactionCount() {
      successfulOutputTransactionCount.incrementAndGet();
    }

    public int getSuccessfulOutputTransactionCount() {
      return successfulOutputTransactionCount.get();
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
