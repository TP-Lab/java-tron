package org.tron.program.exporter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.tron.common.logsfilter.trigger.InternalTransactionPojo;
import org.tron.common.logsfilter.trigger.LogPojo;
import org.tron.common.logsfilter.trigger.TransactionLogTrigger;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.utils.Sha256Hash;
import org.tron.common.utils.StringUtil;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.protos.Protocol.InternalTransaction;
import org.tron.protos.Protocol.ResourceReceipt;
import org.tron.protos.Protocol.Transaction;
import org.tron.protos.Protocol.TransactionInfo;
import org.tron.protos.contract.AssetIssueContractOuterClass;
import org.tron.protos.contract.BalanceContract.TransferContract;
import org.tron.protos.contract.SmartContractOuterClass;

/**
 * Stateless utility class for building TransactionLogTrigger from transaction data.
 */
@Slf4j(topic = "app")
public class TriggerBuilder {

  private TriggerBuilder() {
    // Utility class, no instantiation
  }

  /**
   * Create TransactionLogTrigger from TransactionInfo and Transaction data
   * with optional TransactionCapsule.
   */
  public static TransactionLogTrigger createTransactionLogTrigger(
      TransactionInfo transactionInfo, Transaction transaction,
      String blockHash, long blockNumber, long timestamp, int transactionIndex,
      TransactionCapsule trxCapsule) {

    TransactionLogTrigger trigger = new TransactionLogTrigger();

    // Basic transaction information - prioritize TransactionCapsule ID if available
    if (trxCapsule != null) {
      try {
        String trxCapsuleId = trxCapsule.getTransactionId().toString();
        trigger.setTransactionId(trxCapsuleId);
        logger.debug("Using transaction ID from TransactionCapsule: {}", trxCapsuleId);
      } catch (Exception e) {
        logger.debug("Could not get transaction ID from TransactionCapsule: {}", e.getMessage());
        // Fall through to other methods
      }
    }

    // If we don't have ID from TransactionCapsule, try TransactionInfo
    if (trigger.getTransactionId() == null && transactionInfo != null) {
      trigger.setTransactionId(Hex.toHexString(transactionInfo.getId().toByteArray()));
      logger.debug("Using transaction ID from TransactionInfo");
    }

    // If still no ID, calculate from transaction data
    if (trigger.getTransactionId() == null && transaction != null) {
      try {
        // Use the correct TRON transaction ID calculation: hash of RawData
        byte[] rawDataBytes = transaction.getRawData().toByteArray();
        Sha256Hash txHash = Sha256Hash.of(CommonParameter.getInstance().isECKeyCryptoEngine(),
            rawDataBytes);
        String calculatedTxId = txHash.toString();
        trigger.setTransactionId(calculatedTxId);
        logger.debug("Calculated transaction ID from RawData: {}", calculatedTxId);
      } catch (Exception e) {
        trigger.setTransactionId("GENESIS_TX_" + transactionIndex);
        logger.debug("Using fallback transaction ID: GENESIS_TX_{}, Error: {}",
            transactionIndex, e.getMessage());
      }
    }

    // Last resort: use a placeholder ID
    if (trigger.getTransactionId() == null) {
      trigger.setTransactionId("UNKNOWN_TX_" + transactionIndex);
      logger.debug("Using unknown transaction ID placeholder");
    }

    trigger.setBlockHash(blockHash);
    trigger.setBlockNumber(blockNumber);
    trigger.setTimeStamp(timestamp);
    trigger.setTransactionIndex(transactionIndex);

    // Transaction data
    if (transaction != null && transaction.getRawData() != null) {
      trigger.setData(Hex.toHexString(transaction.getRawData().getData().toByteArray()));
      trigger.setFeeLimit(transaction.getRawData().getFeeLimit());

      // Initialize extMap if not exists
      if (trigger.getExtMap() == null) {
        trigger.setExtMap(new HashMap<>());
      }

      trigger.setTransactionDetail(transaction.toString());

      // Also store in extMap for backward compatibility
      trigger.getExtMap().put("refBlockNum", transaction.getRawData().getRefBlockNum());
      trigger.getExtMap().put("expiration", transaction.getRawData().getExpiration());
      trigger.getExtMap().put("timestamp", transaction.getRawData().getTimestamp());

      // Authority information (auths field) - contains permission information
      // Note: auths field is rarely used in normal transactions, most permission info is in
      // contract.Permission_id
      try {
        // Try to access auths field, but don't fail if it's not available
        java.lang.reflect.Method getAuthsCountMethod =
            transaction.getRawData().getClass().getMethod("getAuthsCount");
        int authsCount = (Integer) getAuthsCountMethod.invoke(transaction.getRawData());

        if (authsCount > 0) {
          trigger.getExtMap().put("authsCount", (long) authsCount);
          logger.debug("Transaction {} has {} auths entries",
              trigger.getTransactionId(), authsCount);

          // If auths exist, try to process them
          java.lang.reflect.Method getAuthsMethod =
              transaction.getRawData().getClass().getMethod("getAuths", int.class);
          for (int i = 0; i < authsCount; i++) {
            Object auth = getAuthsMethod.invoke(transaction.getRawData(), i);
            trigger.getExtMap().put("auth_" + i + "_exists", 1L);
            logger.debug("Transaction {} - auth_{} exists", trigger.getTransactionId(), i);
          }
        } else {
          logger.debug("Transaction {} has no auths field data", trigger.getTransactionId());
        }
      } catch (Exception e) {
        // auths field might not be available in this TRON version or protobuf definition
        logger.debug("Transaction {} - auths field not available or accessible: {}",
            trigger.getTransactionId(), e.getMessage());
      }

      // Contract information
      if (transaction.getRawData().getContractCount() > 0) {
        Transaction.Contract contract = transaction.getRawData().getContract(0);
        trigger.setContractType(contract.getType().toString());
        trigger.setContractCallValue(getCallValue(contract));
        trigger.setContractData(contract.getParameter().toString());

        // Permission_id is already in the transaction object
        if (contract.getPermissionId() > 0) {
          logger.debug("Transaction {} - Permission_id: {}",
              trigger.getTransactionId(), contract.getPermissionId());
        }

        // Extract transfer information for transfer contracts
        extractTransferInfo(trigger, contract);
      }
    }

    // Transaction signatures
    if (transaction != null && transaction.getSignatureCount() > 0) {
      List<String> signatures = new ArrayList<>();
      for (int i = 0; i < transaction.getSignatureCount(); i++) {
        String signature = Hex.toHexString(transaction.getSignature(i).toByteArray());
        signatures.add(signature);
      }
      trigger.setSignatures(signatures);
    }

    // Transaction execution results and fees
    if (transactionInfo != null) {
      // Result information
      if (transactionInfo.getResult() != TransactionInfo.code.SUCESS) {
        trigger.setResult(transactionInfo.getResult().toString());
        trigger.setTxResult(transactionInfo.getResult().toString());
      } else {
        trigger.setResult("SUCCESS");
        trigger.setTxResult("SUCCESS");
      }

      // Total fee
      trigger.setFee(transactionInfo.getFee());

      // Use protobuf encoding for txResult - create a Transaction.Result structure
      try {
        Transaction.Result.Builder resultBuilder = Transaction.Result.newBuilder();

        // Set basic fields
        resultBuilder.setFee(transactionInfo.getFee());

        // Map TransactionInfo.code to Transaction.Result.code
        if (transactionInfo.getResult() == TransactionInfo.code.SUCESS) {
          resultBuilder.setRet(Transaction.Result.code.SUCESS);
        } else {
          resultBuilder.setRet(Transaction.Result.code.FAILED);
        }

        // Set default contract result
        resultBuilder.setContractRet(Transaction.Result.contractResult.SUCCESS);

        // Add withdraw and unfreeze amounts if available
        if (transactionInfo.getWithdrawAmount() > 0) {
          resultBuilder.setWithdrawAmount(transactionInfo.getWithdrawAmount());
        }
        if (transactionInfo.getUnfreezeAmount() > 0) {
          resultBuilder.setUnfreezeAmount(transactionInfo.getUnfreezeAmount());
        }

        // Add exchange-related amounts if available
        if (transactionInfo.getExchangeReceivedAmount() > 0) {
          resultBuilder.setExchangeReceivedAmount(transactionInfo.getExchangeReceivedAmount());
        }
        if (transactionInfo.getExchangeInjectAnotherAmount() > 0) {
          resultBuilder.setExchangeInjectAnotherAmount(
              transactionInfo.getExchangeInjectAnotherAmount());
        }
        if (transactionInfo.getExchangeWithdrawAnotherAmount() > 0) {
          resultBuilder.setExchangeWithdrawAnotherAmount(
              transactionInfo.getExchangeWithdrawAnotherAmount());
        }
        if (transactionInfo.getExchangeId() > 0) {
          resultBuilder.setExchangeId(transactionInfo.getExchangeId());
        }

        // Add asset issue ID if available
        if (!transactionInfo.getAssetIssueID().isEmpty()) {
          resultBuilder.setAssetIssueID(transactionInfo.getAssetIssueID());
        }

        // Add shielded transaction fee if available
        if (transactionInfo.getShieldedTransactionFee() > 0) {
          resultBuilder.setShieldedTransactionFee(transactionInfo.getShieldedTransactionFee());
        }

        // Build the protobuf result and use its toString() method for structured output
        Transaction.Result txResult = resultBuilder.build();
        trigger.setTxResult(txResult.toString());

      } catch (Exception e) {
        logger.warn("Failed to create protobuf Transaction.Result, "
            + "falling back to simple format: {}", e.getMessage());
        // Fallback to simple format if protobuf creation fails
        trigger.setTxResult("fee:" + transactionInfo.getFee()
            + ",result:" + transactionInfo.getResult().toString());
      }
    }

    // Note: Transaction.Result is not available in database-stored transactions
    // (getRetCount() = 0). All execution result information is available in
    // TransactionInfo instead.

    if (transactionInfo != null) {

      // Fee and energy information
      if (transactionInfo.hasReceipt()) {
        ResourceReceipt receipt = transactionInfo.getReceipt();
        trigger.setEnergyUsage(receipt.getEnergyUsage());
        trigger.setEnergyFee(receipt.getEnergyFee());
        trigger.setOriginEnergyUsage(receipt.getOriginEnergyUsage());
        trigger.setEnergyUsageTotal(receipt.getEnergyUsageTotal());
        trigger.setNetUsage(receipt.getNetUsage());
        trigger.setNetFee(receipt.getNetFee());
      }

      // Contract result
      if (transactionInfo.getContractResultCount() > 0) {
        trigger.setContractResult(
            Hex.toHexString(transactionInfo.getContractResult(0).toByteArray()));
      }

      // Contract address
      if (transactionInfo.getContractAddress() != null
          && !transactionInfo.getContractAddress().isEmpty()) {
        trigger.setContractAddress(
            StringUtil.encode58Check(transactionInfo.getContractAddress().toByteArray()));
      }

      // Internal transactions
      if (transactionInfo.getInternalTransactionsCount() > 0) {
        List<InternalTransactionPojo> internalTxList = new ArrayList<>();
        for (InternalTransaction internalTx : transactionInfo.getInternalTransactionsList()) {
          InternalTransactionPojo pojo = new InternalTransactionPojo();
          pojo.setHash(Hex.toHexString(internalTx.getHash().toByteArray()));

          // Handle CallValueInfo
          if (internalTx.getCallValueInfoCount() > 0) {
            InternalTransaction.CallValueInfo callValueInfo = internalTx.getCallValueInfo(0);
            pojo.setCallValue(callValueInfo.getCallValue());
            if (!callValueInfo.getTokenId().isEmpty()) {
              pojo.getTokenInfo().put(callValueInfo.getTokenId(), callValueInfo.getCallValue());
            }
          }

          pojo.setCaller_address(
              StringUtil.encode58Check(internalTx.getCallerAddress().toByteArray()));
          pojo.setTransferTo_address(
              StringUtil.encode58Check(internalTx.getTransferToAddress().toByteArray()));

          // Handle extra data
          if (!internalTx.getExtra().isEmpty()) {
            pojo.setData(internalTx.getExtra());
          }

          pojo.setRejected(internalTx.getRejected());
          pojo.setNote(internalTx.getNote().toStringUtf8());
          internalTxList.add(pojo);
        }
        trigger.setInternalTransactionList(internalTxList);
      }

      // Logs
      if (transactionInfo.getLogCount() > 0) {
        List<LogPojo> logList = new ArrayList<>();
        for (int i = 0; i < transactionInfo.getLogCount(); i++) {
          TransactionInfo.Log log = transactionInfo.getLog(i);
          LogPojo logPojo = new LogPojo();

          logPojo.setAddress(Hex.toHexString(log.getAddress().toByteArray()));
          logPojo.setBlockHash(blockHash);
          logPojo.setBlockNumber(blockNumber);
          logPojo.setData(Hex.toHexString(log.getData().toByteArray()));
          logPojo.setLogIndex(i);
          logPojo.setTransactionHash(trigger.getTransactionId());
          logPojo.setTransactionIndex(transactionIndex);

          // Topics
          List<String> topics = new ArrayList<>();
          for (int j = 0; j < log.getTopicsCount(); j++) {
            topics.add(Hex.toHexString(log.getTopics(j).toByteArray()));
          }
          logPojo.setTopicList(topics);

          logList.add(logPojo);
        }
        trigger.setLogList(logList);
      }

      // Memo fee and multi-sign fee - setting to 0 as they're not directly available
      // in the public TransactionInfo API
      trigger.setMemoFee(0);
      trigger.setMultiSignFee(0);

      // Energy unit price - this might need to be retrieved from chain parameters
      trigger.setEnergyUnitPrice(0);

      // Cumulative energy used - for a single transaction, same as energyUsageTotal
      if (transactionInfo.hasReceipt()) {
        trigger.setCumulativeEnergyUsed(transactionInfo.getReceipt().getEnergyUsageTotal());
      }

      // Pre-cumulative log count - requires context of previous transactions
      trigger.setPreCumulativeLogCount(0);

      // Latest solidified block number - using the current block number as a placeholder
      trigger.setLatestSolidifiedBlockNumber(blockNumber);

      // ExtMap - merge getCancelUnfreezeV2AmountMap() from TransactionInfo with existing extMap
      // IMPORTANT: Don't overwrite existing extMap, merge the data instead
      if (trigger.getExtMap() == null) {
        trigger.setExtMap(new HashMap<>());
      }

      if (transactionInfo.getCancelUnfreezeV2AmountCount() > 0) {
        Map<String, Long> cancelUnfreezeMap = transactionInfo.getCancelUnfreezeV2AmountMap();
        trigger.getExtMap().putAll(cancelUnfreezeMap);
        logger.debug("Transaction {} - Added {} cancel unfreeze entries to extMap",
            trigger.getTransactionId(), cancelUnfreezeMap.size());
      }

      // Asset-related fields
      if (!transactionInfo.getAssetIssueID().isEmpty()) {
        trigger.getExtMap().put("assetIssueID",
            Long.parseLong(transactionInfo.getAssetIssueID()));
      }

      // Withdraw and unfreeze amounts
      if (transactionInfo.getWithdrawAmount() > 0) {
        trigger.getExtMap().put("withdrawAmount", transactionInfo.getWithdrawAmount());
      }

      if (transactionInfo.getUnfreezeAmount() > 0) {
        trigger.getExtMap().put("unfreezeAmount", transactionInfo.getUnfreezeAmount());
      }

      if (transactionInfo.getWithdrawExpireAmount() > 0) {
        trigger.getExtMap().put("withdrawExpireAmount",
            transactionInfo.getWithdrawExpireAmount());
      }

      // Exchange-related fields
      if (transactionInfo.getExchangeId() > 0) {
        trigger.getExtMap().put("exchangeId", transactionInfo.getExchangeId());
        if (transactionInfo.getExchangeReceivedAmount() > 0) {
          trigger.getExtMap().put("exchangeReceivedAmount",
              transactionInfo.getExchangeReceivedAmount());
        }
        if (transactionInfo.getExchangeInjectAnotherAmount() > 0) {
          trigger.getExtMap().put("exchangeInjectAnotherAmount",
              transactionInfo.getExchangeInjectAnotherAmount());
        }
        if (transactionInfo.getExchangeWithdrawAnotherAmount() > 0) {
          trigger.getExtMap().put("exchangeWithdrawAnotherAmount",
              transactionInfo.getExchangeWithdrawAnotherAmount());
        }
      }

      // Shielded transaction fee
      if (transactionInfo.getShieldedTransactionFee() > 0) {
        trigger.getExtMap().put("shieldedTransactionFee",
            transactionInfo.getShieldedTransactionFee());
      }

      // Packing fee
      if (transactionInfo.getPackingFee() > 0) {
        trigger.getExtMap().put("packingFee", transactionInfo.getPackingFee());
      }
    }

    // Log final extMap contents for debugging
    if (trigger.getExtMap() != null && !trigger.getExtMap().isEmpty()) {
      logger.debug("Transaction {} - Final extMap contents: {}",
          trigger.getTransactionId(), trigger.getExtMap());
    } else {
      logger.warn("Transaction {} - extMap is null or empty!", trigger.getTransactionId());
    }

    return trigger;
  }

  /**
   * Extract call value from contract.
   */
  public static long getCallValue(Transaction.Contract contract) {
    try {
      switch (contract.getType()) {
        case TransferContract:
          TransferContract transferContract =
              contract.getParameter().unpack(TransferContract.class);
          return transferContract.getAmount();
        case TriggerSmartContract:
          SmartContractOuterClass.TriggerSmartContract triggerContract =
              contract.getParameter().unpack(
                  SmartContractOuterClass.TriggerSmartContract.class);
          return triggerContract.getCallValue();
        default:
          return 0;
      }
    } catch (Exception e) {
      return 0;
    }
  }

  /**
   * Extract transfer information from contract.
   */
  public static void extractTransferInfo(TransactionLogTrigger trigger,
      Transaction.Contract contract) {
    try {
      switch (contract.getType()) {
        case TransferContract:
          TransferContract transferContract =
              contract.getParameter().unpack(TransferContract.class);
          trigger.setFromAddress(
              StringUtil.encode58Check(transferContract.getOwnerAddress().toByteArray()));
          trigger.setToAddress(
              StringUtil.encode58Check(transferContract.getToAddress().toByteArray()));
          trigger.setAssetAmount(transferContract.getAmount());
          trigger.setAssetName("trx");
          break;
        case TransferAssetContract:
          AssetIssueContractOuterClass.TransferAssetContract transferAssetContract =
              contract.getParameter().unpack(
                  AssetIssueContractOuterClass.TransferAssetContract.class);
          trigger.setFromAddress(
              StringUtil.encode58Check(transferAssetContract.getOwnerAddress().toByteArray()));
          trigger.setToAddress(
              StringUtil.encode58Check(transferAssetContract.getToAddress().toByteArray()));
          trigger.setAssetAmount(transferAssetContract.getAmount());

          // Get TRC10 token ID - the asset_name field contains token ID
          // when ALLOW_SAME_TOKEN_NAME is active
          String tokenId = transferAssetContract.getAssetName().toStringUtf8();
          trigger.setAssetName(tokenId);
          break;
        case TriggerSmartContract:
          SmartContractOuterClass.TriggerSmartContract triggerContract =
              contract.getParameter().unpack(
                  SmartContractOuterClass.TriggerSmartContract.class);
          trigger.setFromAddress(
              StringUtil.encode58Check(triggerContract.getOwnerAddress().toByteArray()));
          trigger.setToAddress(
              StringUtil.encode58Check(triggerContract.getContractAddress().toByteArray()));
          break;
        default:
          // For other contract types, try to extract owner address if available
          break;
      }
    } catch (Exception e) {
      // Ignore extraction errors
    }
  }
}
