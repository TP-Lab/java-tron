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
import lombok.extern.slf4j.Slf4j;
import org.tron.common.logsfilter.trigger.TransactionLogTrigger;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.JsonUtil;
import org.tron.core.ChainBaseManager;
import org.tron.core.Wallet;
import org.tron.core.capsule.TransactionCapsule;
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
  public void processTransactionsConcurrently(
      List<TransactionCapsule> transactions,
      String blockId, long blockNum, long timestamp,
      String outputFormat, boolean useKafka, String kafkaTopic,
      Wallet wallet, ChainBaseManager chainBaseManager,
      KafkaSender kafkaSender) {

    if (transactions.isEmpty()) {
      return;
    }

    logger.debug("Processing {} transactions concurrently...", transactions.size());

    // ====================================================================
    // 第一步：批量预取 TransactionInfo（核心优化点）
    // ====================================================================
    // 将 N 次随机 I/O 转换为 1 次顺序 I/O
    Map<String, TransactionInfo> transactionInfoMap = new HashMap<>();
    try {
      if (chainBaseManager != null && chainBaseManager.getTransactionRetStore() != null) {
        byte[] blockNumKey = ByteArray.fromLong(blockNum);
        org.tron.core.capsule.TransactionRetCapsule transactionRetCapsule =
            chainBaseManager.getTransactionRetStore().getTransactionInfoByBlockNum(blockNumKey);

        if (transactionRetCapsule != null && transactionRetCapsule.getInstance() != null) {
          for (TransactionInfo info
              : transactionRetCapsule.getInstance().getTransactioninfoList()) {
            String txId = ByteArray.toHexString(info.getId().toByteArray());
            transactionInfoMap.put(txId, info);
          }
          logger.debug("Batch prefetched {} TransactionInfo for block {}",
              transactionInfoMap.size(), blockNum);
        } else {
          logger.debug("No TransactionRetCapsule found for block {}, "
              + "falling back to individual queries", blockNum);
        }
      }
    } catch (Exception e) {
      logger.warn("Failed to batch prefetch TransactionInfo for block {}: {}, "
          + "falling back to individual queries", blockNum, e.getMessage());
    }

    // ====================================================================
    // 第二步：构建交易索引映射，避免并发闭包捕获问题
    // ====================================================================
    Map<String, Integer> txIndexMap = new HashMap<>();
    for (int i = 0; i < transactions.size(); i++) {
      txIndexMap.put(transactions.get(i).getTransactionId().toString(), i);
    }

    // ====================================================================
    // 第三步：并发处理每个交易（使用预取的数据）
    // ====================================================================
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    AtomicInteger processedCount = new AtomicInteger(0);

    for (int i = 0; i < transactions.size(); i++) {
      final TransactionCapsule trx = transactions.get(i);
      final String txId = trx.getTransactionId().toString();
      final int transactionIndex = txIndexMap.get(txId);
      final TransactionInfo prefetchedTransactionInfo = transactionInfoMap.get(txId);

      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        try {
          processTransactionWithPrefetchedInfo(trx, prefetchedTransactionInfo, transactionIndex,
              blockId, blockNum, timestamp, outputFormat, useKafka, kafkaTopic, wallet,
              kafkaSender);

          int completed = processedCount.incrementAndGet();
          if (completed % 50 == 0 || completed == transactions.size()) {
            logger.debug("Processed {}/{} transactions in block {}",
                completed, transactions.size(), blockNum);
          }
        } catch (Exception e) {
          logger.error("Error processing transaction {} in block {}: {}",
              transactionIndex + 1, blockNum, e.getMessage(), e);
        }
      }, transactionExecutor);

      futures.add(future);
    }

    // 等待所有交易处理完成
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
      logger.debug("All {} transactions processed successfully in block {}",
          transactions.size(), blockNum);
    } catch (Exception e) {
      logger.error("Error waiting for concurrent transaction processing to complete: {}",
          e.getMessage(), e);
    }
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
      KafkaSender kafkaSender) {

    String txId = trx.getTransactionId().toString();
    logger.debug("Processing transaction #{} (ID: {}) in block {}",
        transactionIndex + 1, txId, blockNum);

    // 第一步：获取 TransactionInfo — 优先使用预取数据（零 I/O）
    TransactionInfo transactionInfo = prefetchedTransactionInfo;
    if (transactionInfo == null) {
      ByteString txIdBytes = ByteString.copyFrom(ByteArray.fromHexString(txId));
      transactionInfo = wallet.getTransactionInfoById(txIdBytes);
      logger.debug("Using fallback query for TransactionInfo (prefetch missed): {}", txId);
    }

    // 第二步：获取 Transaction — 直接从 TransactionCapsule 获取（零 I/O）
    Transaction transaction = null;
    try {
      transaction = trx.getInstance();
      logger.debug("Retrieved transaction directly from TransactionCapsule for ID: {}", txId);
    } catch (Exception e) {
      logger.warn("Could not get transaction from TransactionCapsule for ID {}: {}",
          txId, e.getMessage());
      // 降级处理：仅在 TransactionCapsule 失败时才查询数据库
      ByteString txIdBytes = ByteString.copyFrom(ByteArray.fromHexString(txId));
      transaction = wallet.getTransactionById(txIdBytes);
    }

    // 第三步：处理交易数据
    processTransactionData(transactionInfo, transaction, trx, transactionIndex,
        blockId, blockNum, timestamp, outputFormat, useKafka, kafkaTopic, kafkaSender);
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
        blockId, blockNum, timestamp, outputFormat, useKafka, kafkaTopic, wallet, kafkaSender);
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
      KafkaSender kafkaSender) {

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
          TransactionLogTrigger trigger = TriggerBuilder.createTransactionLogTrigger(
              transactionInfo, transaction, blockId, blockNum, timestamp,
              transactionIndex, trx);
          TransactionLogTriggerProtos.TransactionLogTriggerPB protoMsg =
              TriggerProtoConverter.convert(trigger);
          byte[] payload = protoMsg.toByteArray();

          if (kafkaTopicToUse != null && kafkaSender != null
              && kafkaSender.hasBytesProducer()) {
            String key = kafkaKey != null ? kafkaKey : trigger.getTransactionId();
            if (isValidTransactionId(trigger.getTransactionId())) {
              kafkaSender.sendBytes(kafkaTopicToUse, key, payload);
            } else {
              logger.warn("Skipped Kafka proto send due to invalid transaction ID: {}",
                  trigger.getTransactionId());
            }
          } else {
            // 没有 Kafka 时输出 base64 到控制台
            System.out.println(Base64.getEncoder().encodeToString(payload));
          }
        } catch (Exception e) {
          logger.error("Error creating protobuf TransactionLogTrigger: " + e.getMessage(), e);
        }
      } else {
        // ---- trigger: JSON 输出 ----
        try {
          TransactionLogTrigger trigger = TriggerBuilder.createTransactionLogTrigger(
              transactionInfo, transaction, blockId, blockNum, timestamp,
              transactionIndex, trx);
          String jsonOutput = JsonUtil.obj2Json(trigger);

          if (jsonOutput != null) {
            boolean sentToKafka = false;

            if (kafkaTopicToUse != null && kafkaSender != null
                && kafkaSender.hasStringProducer()) {
              String key = kafkaKey != null ? kafkaKey : trigger.getTransactionId();
              if (isValidTransactionId(trigger.getTransactionId())) {
                kafkaSender.send(kafkaTopicToUse, key, jsonOutput);
                sentToKafka = true;
              } else {
                logger.warn("Skipped Kafka send due to invalid transaction ID: {}",
                    trigger.getTransactionId());
              }
            }

            // 未发送到 Kafka 时输出到控制台
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
    } else {
      // ---- 传统格式（json / protobuf / both）----
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
