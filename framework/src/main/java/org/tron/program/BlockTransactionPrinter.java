package org.tron.program;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.text.SimpleDateFormat;
import java.util.Date;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.tron.common.application.Application;
import org.tron.common.application.ApplicationFactory;
import org.tron.common.application.TronApplicationContext;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.StringUtil;
import org.tron.core.ChainBaseManager;
import org.tron.core.Constant;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.protos.Protocol.Transaction;
import org.tron.protos.Protocol.Transaction.Contract.ContractType;
import org.tron.protos.Protocol.Transaction.Result;
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
      System.out.println("Usage: BlockTransactionPrinter <startBlockNum> <endBlockNum> [options]");
      System.out.println("Options:");
      System.out.println("  -c <config_file>: Specify a custom configuration file");
      System.out.println("  -d <data_dir>: Specify a custom data directory");
      return;
    }

    try {
      long startBlockNum = Long.parseLong(args[0]);
      long endBlockNum = Long.parseLong(args[1]);

      if (startBlockNum < 0 || endBlockNum < 0 || startBlockNum > endBlockNum) {
        System.out.println("Invalid block range. Start block must be <= end block and both must be >= 0");
        return;
      }

      // Initialize TRON environment
      // Create a new array without the block numbers for Args.setParam
      String[] configArgs;
      if (args.length > 2) {
        configArgs = new String[args.length - 2];
        System.arraycopy(args, 2, configArgs, 0, args.length - 2);
      } else {
        configArgs = new String[0];
      }
      Args.setParam(configArgs, Constant.TESTNET_CONF);
      DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
      beanFactory.setAllowCircularReferences(false);
      TronApplicationContext context = new TronApplicationContext(beanFactory);
      context.register(DefaultConfig.class);
      context.refresh();
      Application appT = ApplicationFactory.create(context);
      appT.startup();

      // Get ChainBaseManager instance
      ChainBaseManager chainBaseManager = context.getBean(ChainBaseManager.class);

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
              Transaction transaction = trx.getInstance();
              String txId = trx.getTransactionId().toString();

              // Get transaction status if available
              String status = "Unknown";
              if (transaction.getRetCount() > 0) {
                Result.contractResult contractResult = transaction.getRet(0).getContractRet();
                status = contractResult.toString();
              }

              System.out.println("  Transaction #" + (i + 1) + ":");
              System.out.println("    ID: " + txId);
              System.out.println("    Status: " + status);

              // Print transaction details
              if (transaction.getRawData().getContractCount() > 0) {
                Transaction.Contract contract = transaction.getRawData().getContract(0);
                ContractType contractType = contract.getType();
                System.out.println("    Type: " + contractType);

                // Print owner address if available
                byte[] ownerAddress = trx.getOwnerAddress();
                if (ownerAddress != null && ownerAddress.length > 0) {
                  System.out.println("    From: " + StringUtil.encode58Check(ownerAddress));
                }

                // Print to address if available
                byte[] toAddress = TransactionCapsule.getToAddress(contract);
                if (toAddress != null && toAddress.length > 0) {
                  System.out.println("    To: " + StringUtil.encode58Check(toAddress));
                }

                // Print additional details based on contract type
                try {
                  if (contractType == ContractType.TransferContract) {
                    TransferContract transferContract = contract.getParameter()
                        .unpack(TransferContract.class);
                    long amount = transferContract.getAmount();
                    System.out.println("    Amount: " + amount + " SUN (" + (amount / 1_000_000.0) + " TRX)");
                  } else if (contractType == ContractType.TriggerSmartContract) {
                    System.out.println("    Call Value: " + TransactionCapsule.getCallValue(contract) + " SUN");
                    // Could add more details about the smart contract call here
                  }
                  // Add more contract types as needed
                } catch (InvalidProtocolBufferException e) {
                  System.out.println("    Error parsing contract details: " + e.getMessage());
                }
              }

              // Print timestamp
              System.out.println("    Timestamp: " + dateFormat.format(new Date(transaction.getRawData().getTimestamp())));

              // Print expiration
              System.out.println("    Expiration: " + dateFormat.format(new Date(transaction.getRawData().getExpiration())));
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

      // Shutdown the application
      appT.shutdown();
      System.out.println("\nTransaction printing completed successfully.");

    } catch (NumberFormatException e) {
      System.out.println("Error: Block numbers must be valid integers");
    } catch (Exception e) {
      System.out.println("Error: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
