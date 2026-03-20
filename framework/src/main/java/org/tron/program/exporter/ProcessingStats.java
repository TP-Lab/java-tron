package org.tron.program.exporter;

import lombok.extern.slf4j.Slf4j;

/**
 * 处理统计追踪 — 记录区块/交易处理速率
 */
@Slf4j(topic = "app")
public class ProcessingStats {

  private long startTime;
  private long totalBlocksProcessed;
  private long totalTransactionsProcessed;
  private long lastStatsTime;
  private long lastBlocksProcessed;
  private long lastTransactionsProcessed;
  private long startBlockNumber;
  private long currentBlockNumber;

  public void initialize(long startBlock) {
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

  public void update(int blocksProcessed, int transactionsProcessed, long latestBlockNumber) {
    totalBlocksProcessed += blocksProcessed;
    totalTransactionsProcessed += transactionsProcessed;
    currentBlockNumber = latestBlockNumber;

    long currentTime = System.currentTimeMillis();
    long timeSinceLastStats = currentTime - lastStatsTime;

    if (timeSinceLastStats >= 10000 || totalBlocksProcessed - lastBlocksProcessed >= 100) {
      long totalTime = currentTime - startTime;
      double totalTimeSeconds = totalTime / 1000.0;
      double intervalTimeSeconds = timeSinceLastStats / 1000.0;

      double overallBlocksPerSecond = totalTimeSeconds > 0 ? totalBlocksProcessed / totalTimeSeconds : 0;
      double overallTransactionsPerSecond = totalTimeSeconds > 0 ? totalTransactionsProcessed / totalTimeSeconds : 0;

      long intervalBlocks = totalBlocksProcessed - lastBlocksProcessed;
      long intervalTransactions = totalTransactionsProcessed - lastTransactionsProcessed;
      double intervalBlocksPerSecond = intervalTimeSeconds > 0 ? intervalBlocks / intervalTimeSeconds : 0;
      double intervalTransactionsPerSecond = intervalTimeSeconds > 0 ? intervalTransactions / intervalTimeSeconds : 0;

      String statsMessage = String.format(
        "Processing Statistics - Current Block: %d (Started from: %d) | Total: %d blocks, %d transactions in %.1fs "
        + "(%.2f blocks/s, %.2f tx/s) | Recent: %d blocks, %d transactions in %.1fs "
        + "(%.2f blocks/s, %.2f tx/s)",
        currentBlockNumber, startBlockNumber,
        totalBlocksProcessed, totalTransactionsProcessed, totalTimeSeconds,
        overallBlocksPerSecond, overallTransactionsPerSecond,
        intervalBlocks, intervalTransactions, intervalTimeSeconds,
        intervalBlocksPerSecond, intervalTransactionsPerSecond
      );

      System.out.println("=== " + statsMessage + " ===");
      logger.info(statsMessage);

      lastStatsTime = currentTime;
      lastBlocksProcessed = totalBlocksProcessed;
      lastTransactionsProcessed = totalTransactionsProcessed;
    }
  }

  public void logFinal() {
    long totalTime = System.currentTimeMillis() - startTime;
    double totalTimeSeconds = totalTime / 1000.0;
    double overallBlocksPerSecond = totalTimeSeconds > 0 ? totalBlocksProcessed / totalTimeSeconds : 0;
    double overallTransactionsPerSecond = totalTimeSeconds > 0 ? totalTransactionsProcessed / totalTimeSeconds : 0;

    String finalStatsMessage = String.format(
      "Final Statistics - Block Range: %d to %d | Processed %d blocks and %d transactions in %.1f seconds "
      + "(Average: %.2f blocks/s, %.2f transactions/s)",
      startBlockNumber, currentBlockNumber,
      totalBlocksProcessed, totalTransactionsProcessed, totalTimeSeconds,
      overallBlocksPerSecond, overallTransactionsPerSecond
    );

    System.out.println("=== " + finalStatsMessage + " ===");
    logger.info(finalStatsMessage);
  }

  public long getTotalBlocksProcessed() {
    return totalBlocksProcessed;
  }

  public long getTotalTransactionsProcessed() {
    return totalTransactionsProcessed;
  }
}
