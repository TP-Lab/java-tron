package org.tron.program;

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
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.protos.Protocol.TransactionInfo;
import com.google.protobuf.ByteString;

/**
 * 检查指定区块的内部交易数据
 */
@Slf4j(topic = "app")
public class CheckInternalTx {

  public static void main(String[] args) {
    if (args.length < 1) {
      System.out.println("Usage: CheckInternalTx <blockNumber> [options]");
      System.out.println("Options:");
      System.out.println("  -c <config_file>: Specify a custom configuration file");
      System.out.println("  -d <data_dir>: Specify a custom data directory");
      return;
    }

    try {
      long blockNum = Long.parseLong(args[0]);
      
      // Disable unnecessary services
      CommonParameter.getInstance().setNeedToUpdateAsset(false);
      CommonParameter.getInstance().setP2pDisable(true);
      CommonParameter.getInstance().setRpcEnable(false);
      CommonParameter.getInstance().setEventSubscribe(false);
      
      // Enable read-only mode
      System.setProperty("database.readonly", "true");
      
      // Parse remaining args for TRON config
      String[] configArgs = new String[args.length - 1];
      System.arraycopy(args, 1, configArgs, 0, args.length - 1);
      Args.setParam(configArgs, Constant.TESTNET_CONF);
      
      // Initialize context
      DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
      beanFactory.setAllowCircularReferences(false);
      TronApplicationContext context = new TronApplicationContext(beanFactory);
      context.register(DefaultConfig.class);
      context.refresh();
      
      ChainBaseManager chainBaseManager = context.getBean(ChainBaseManager.class);
      Wallet wallet = context.getBean(Wallet.class);
      
      // Check configuration
      boolean saveInternalTx = CommonParameter.getInstance().isSaveInternalTx();
      boolean saveFeaturedInternalTx = CommonParameter.getInstance().isSaveFeaturedInternalTx();
      
      System.out.println("=== Internal Transaction Configuration ===");
      System.out.println("saveInternalTx: " + saveInternalTx);
      System.out.println("saveFeaturedInternalTx: " + saveFeaturedInternalTx);
      System.out.println();
      
      // Get block
      BlockCapsule block = chainBaseManager.getBlockByNum(blockNum);
      if (block == null) {
        System.out.println("Block " + blockNum + " not found!");
        return;
      }
      
      System.out.println("=== Block " + blockNum + " ===");
      System.out.println("Block Hash: " + block.getBlockId().toString());
      System.out.println("Transactions: " + block.getTransactions().size());
      System.out.println();
      
      // Check each transaction
      int txIndex = 0;
      int totalInternalTx = 0;
      
      for (TransactionCapsule trx : block.getTransactions()) {
        String txId = trx.getTransactionId().toString();
        
        // Get transaction info
        ByteString txIdBytes = ByteString.copyFrom(ByteArray.fromHexString(txId));
        TransactionInfo txInfo = wallet.getTransactionInfoById(txIdBytes);
        
        if (txInfo != null) {
          int internalTxCount = txInfo.getInternalTransactionsCount();
          totalInternalTx += internalTxCount;
          
          System.out.println("Transaction #" + txIndex + ": " + txId);
          System.out.println("  Contract Type: " + trx.getInstance().getRawData().getContract(0).getType());
          System.out.println("  Internal Transactions: " + internalTxCount);
          
          if (internalTxCount > 0) {
            System.out.println("  Internal TX Details:");
            for (int i = 0; i < internalTxCount; i++) {
              Protocol.InternalTransaction internalTx = txInfo.getInternalTransactions(i);
              System.out.println("    [" + i + "] Hash: " + org.bouncycastle.util.encoders.Hex.toHexString(internalTx.getHash().toByteArray()));
              System.out.println("        Rejected: " + internalTx.getRejected());
              System.out.println("        Note: " + internalTx.getNote().toStringUtf8());
            }
          }
          System.out.println();
        } else {
          System.out.println("Transaction #" + txIndex + ": " + txId);
          System.out.println("  TransactionInfo not found!");
          System.out.println();
        }
        
        txIndex++;
      }
      
      System.out.println("=== Summary ===");
      System.out.println("Total Transactions: " + block.getTransactions().size());
      System.out.println("Total Internal Transactions: " + totalInternalTx);
      
      if (totalInternalTx == 0 && !saveInternalTx) {
        System.out.println();
        System.out.println("WARNING: No internal transactions found and saveInternalTx is disabled!");
        System.out.println("To save internal transactions for future blocks, add to config.conf:");
        System.out.println("  vm {");
        System.out.println("    saveInternalTx = true");
        System.out.println("  }");
      }
      
      context.close();
      
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
    }
  }
}

