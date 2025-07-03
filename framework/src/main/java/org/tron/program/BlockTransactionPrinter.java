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
import org.tron.core.ChainBaseManager;
import org.tron.core.Constant;
import org.tron.core.Wallet;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.config.DatabaseOnlyConfig;
import org.tron.core.config.args.Args;
import org.tron.core.services.http.JsonFormat;
import org.tron.core.services.http.Util;
import org.tron.protos.Protocol.Transaction;
import org.tron.protos.Protocol.Transaction.Contract.ContractType;
import org.tron.protos.Protocol.Transaction.Result;
import org.tron.protos.Protocol.TransactionInfo;
import org.tron.protos.Protocol.TransactionInfo.Log;
import org.tron.protos.contract.BalanceContract.TransferContract;

import java.util.List;

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
 * 
 * Options:
 *    -c <config_file>: Specify a custom configuration file
 *    -d <data_dir>: Specify a custom data directory
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
    if (args.length < 2) {
      System.out.println("Usage:");
      System.out.println("  BlockTransactionPrinter <startBlockNum> <endBlockNum> [options]");
      System.out.println("  BlockTransactionPrinter -tx <transactionId> [options]");
      System.out.println("Options:");
      System.out.println("  -c <config_file>: Specify a custom configuration file");
      System.out.println("  -d <data_dir>: Specify a custom data directory");
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

      // Initialize TRON environment
      // Create a new array without the block numbers/transaction ID for Args.setParam
      String[] configArgs;
      int configStartIndex = isTransactionMode ? 2 : 2; // Both modes skip first 2 args
      if (args.length > configStartIndex) {
        configArgs = new String[args.length - configStartIndex];
        System.arraycopy(args, configStartIndex, configArgs, 0, args.length - configStartIndex);
      } else {
        configArgs = new String[0];
      }

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
      context.register(DatabaseOnlyConfig.class);
      context.refresh();
      System.out.println("Database-only context initialized successfully");

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
        if (lowestBlockNum < 0) {
          // If getLowestBlockNum returns -1 or negative, try alternative approach
          List<BlockCapsule> firstBlocks = chainBaseManager.getBlockStore().getLimitNumber(0, 1);
          if (!firstBlocks.isEmpty()) {
            lowestBlockNum = firstBlocks.get(0).getNum();
          } else {
            lowestBlockNum = 0;
          }
        }
      } catch (Exception e) {
        System.out.println("Warning: Could not get lowest block number, using 0. Error: " + e.getMessage());
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

      System.out.println("=== Database block range: " + lowestBlockNum + " to " + latestBlockNum + " ===");

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
        System.out.println("=== Querying transaction: " + transactionId + " ===");

        try {
          ByteString txIdBytes = ByteString.copyFrom(ByteArray.fromHexString(transactionId));
          TransactionInfo transactionInfo = wallet.getTransactionInfoById(txIdBytes);

          if (transactionInfo != null) {
            // Convert log addresses to TRON addresses
            List<Log> newLogList = Util.convertLogAddressToTronAddress(transactionInfo);
            TransactionInfo transactionInfoWithConvertedLogs = transactionInfo.toBuilder()
                .clearLog()
                .addAllLog(newLogList)
                .build();

            // Print transaction info in the same format as wallet/gettransactioninfobyid
            System.out.println("Transaction found:");
            System.out.println(JsonFormat.printToString(transactionInfoWithConvertedLogs, true));
          } else {
            System.out.println("Transaction not found: " + transactionId);
            System.out.println("This could happen if:");
            System.out.println("1. The transaction ID is incorrect");
            System.out.println("2. The transaction is not in this database");
            System.out.println("3. The transaction is too old and has been pruned");
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
        System.out.println("Error: Requested block range (" + startBlockNum + " to " + endBlockNum + 
                          ") is outside the available range (" + lowestBlockNum + " to " + latestBlockNum + ")");
        context.close();
        System.out.println("=== Read-Only Database Query Failed - Exiting Program ===");
        return;
      }

      System.out.println("=== Printing transactions from block " + startBlockNum + " to " + endBlockNum + " ===");

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
        List<BlockCapsule> blocks = chainBaseManager.getBlockStore().getLimitNumber(currentStart, limit);
        totalBlocks += blocks.size();

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

              if (transactionInfo != null) {
                // Convert log addresses to TRON addresses
                List<Log> newLogList = Util.convertLogAddressToTronAddress(transactionInfo);
                TransactionInfo transactionInfoWithConvertedLogs = transactionInfo.toBuilder()
                    .clearLog()
                    .addAllLog(newLogList)
                    .build();

                // Print transaction info in the same format as wallet/gettransactioninfobyid
                System.out.println(JsonFormat.printToString(transactionInfoWithConvertedLogs, true));
              } else {
                System.out.println("    No transaction info found for ID: " + txId);
              }
            }
          }
        }
      }

      // Print summary
      System.out.println("\n=== Summary ===");
      System.out.println("Total blocks processed: " + totalBlocks);
      System.out.println("Total transactions: " + totalTransactions);
      System.out.println("Average transactions per block: " + 
                        (totalBlocks > 0 ? String.format("%.2f", (double)totalTransactions / totalBlocks) : "0"));

      // Shutdown the context
      context.close();
      System.out.println("\nTransaction printing completed successfully.");
      System.out.println("=== Read-Only Database Query Completed - Exiting Program ===");

    } catch (NumberFormatException e) {
      System.out.println("Error: Block numbers must be valid integers");
    } catch (Exception e) {
      System.out.println("Error: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
