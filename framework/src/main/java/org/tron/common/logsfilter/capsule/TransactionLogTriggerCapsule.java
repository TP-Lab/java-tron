package org.tron.common.logsfilter.capsule;

import static org.tron.protos.Protocol.Transaction.Contract.ContractType.CreateSmartContract;
import static org.tron.protos.Protocol.Transaction.Contract.ContractType.TransferAssetContract;
import static org.tron.protos.Protocol.Transaction.Contract.ContractType.TransferContract;
import static org.tron.protos.Protocol.Transaction.Contract.ContractType.TriggerSmartContract;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;

import java.util.*;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.spongycastle.util.encoders.Hex;
import org.tron.common.logsfilter.EventPluginLoader;
import org.tron.common.logsfilter.trigger.ContractTrigger;
import org.tron.common.logsfilter.trigger.InternalTransactionPojo;
import org.tron.common.logsfilter.trigger.TransactionLogTrigger;
import org.tron.common.runtime.InternalTransaction;
import org.tron.common.runtime.ProgramResult;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.WalletUtil;
import org.tron.core.Wallet;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.db.TransactionTrace;
import org.tron.protos.Protocol;
import org.tron.protos.contract.AssetIssueContractOuterClass.TransferAssetContract;
import org.tron.protos.contract.BalanceContract.TransferContract;
import org.tron.protos.contract.SmartContractOuterClass;
import org.tron.protos.contract.SmartContractOuterClass.CreateSmartContract;
import org.tron.protos.contract.SmartContractOuterClass.TriggerSmartContract;

@Slf4j
public class TransactionLogTriggerCapsule extends TriggerCapsule {

  @Getter
  @Setter
  TransactionLogTrigger transactionLogTrigger;

  public TransactionLogTriggerCapsule(TransactionCapsule trxCasule, BlockCapsule blockCapsule) {
    transactionLogTrigger = new TransactionLogTrigger();
    if (Objects.nonNull(blockCapsule)) {
      transactionLogTrigger.setBlockHash(blockCapsule.getBlockId().toString());
    }
    transactionLogTrigger.setTransactionId(trxCasule.getTransactionId().toString());
    transactionLogTrigger.setTimeStamp(blockCapsule.getTimeStamp());
    transactionLogTrigger.setBlockNumber(trxCasule.getBlockNum());
    transactionLogTrigger.setData(Hex.toHexString(trxCasule
        .getInstance().getRawData().getData().toByteArray()));

    TransactionTrace trxTrace = trxCasule.getTrxTrace();

    //result
    if (Objects.nonNull(trxCasule.getContractRet())) {
      transactionLogTrigger.setResult(trxCasule.getContractRet().toString());
    }

    if (Objects.nonNull(trxCasule.getInstance().getRawData())) {
      // feelimit
      transactionLogTrigger.setFeeLimit(trxCasule.getInstance().getRawData().getFeeLimit());

      Protocol.Transaction.Contract contract = trxCasule.getInstance().getRawData().getContract(0);
      Any contractParameter = null;
      // contract type
      if (Objects.nonNull(contract)) {
        Protocol.Transaction.Contract.ContractType contractType = contract.getType();
        if (Objects.nonNull(contractType)) {
          transactionLogTrigger.setContractType(contractType.toString());
        }

        contractParameter = contract.getParameter();

        transactionLogTrigger.setContractCallValue(TransactionCapsule.getCallValue(contract));
      }

      if (Objects.nonNull(contractParameter) && Objects.nonNull(contract)) {
        try {
          if (contract.getType() == TransferContract) {
            TransferContract contractTransfer = contractParameter.unpack(TransferContract.class);

            if (Objects.nonNull(contractTransfer)) {
              transactionLogTrigger.setAssetName("trx");

              if (Objects.nonNull(contractTransfer.getOwnerAddress())) {
                transactionLogTrigger.setFromAddress(
                    WalletUtil.encode58Check(contractTransfer.getOwnerAddress().toByteArray()));
              }

              if (Objects.nonNull(contractTransfer.getToAddress())) {
                transactionLogTrigger.setToAddress(
                    WalletUtil.encode58Check(contractTransfer.getToAddress().toByteArray()));
              }

              transactionLogTrigger.setAssetAmount(contractTransfer.getAmount());
            }

          } else if (contract.getType() == TransferAssetContract) {
            TransferAssetContract contractTransfer = contractParameter
                .unpack(TransferAssetContract.class);

            if (Objects.nonNull(contractTransfer)) {
              if (Objects.nonNull(contractTransfer.getAssetName())) {
                transactionLogTrigger.setAssetName(contractTransfer.getAssetName().toStringUtf8());
              }

              if (Objects.nonNull(contractTransfer.getOwnerAddress())) {
                transactionLogTrigger.setFromAddress(
                    WalletUtil.encode58Check(contractTransfer.getOwnerAddress().toByteArray()));
              }

              if (Objects.nonNull(contractTransfer.getToAddress())) {
                transactionLogTrigger.setToAddress(
                    WalletUtil.encode58Check(contractTransfer.getToAddress().toByteArray()));
              }
              transactionLogTrigger.setAssetAmount(contractTransfer.getAmount());
            }
          } else if (contract.getType() == TriggerSmartContract) {
            TriggerSmartContract triggerSmartContract = contractParameter
                    .unpack(TriggerSmartContract.class);

            if (Objects.nonNull(triggerSmartContract)) {
              transactionLogTrigger.setAssetName("");
              transactionLogTrigger.setTokenId(triggerSmartContract.getTokenId());

              if (Objects.nonNull(triggerSmartContract.getOwnerAddress())) {
                transactionLogTrigger.setFromAddress(
                        WalletUtil.encode58Check(triggerSmartContract.getOwnerAddress().toByteArray()));
              }

              if (Objects.nonNull(triggerSmartContract.getContractAddress())) {
                transactionLogTrigger.setToAddress(
                        WalletUtil.encode58Check(triggerSmartContract.getContractAddress().toByteArray()));
              }
              transactionLogTrigger.setAssetAmount(triggerSmartContract.getCallValue());
              transactionLogTrigger.setAssetTokenAmount(triggerSmartContract.getCallTokenValue());
            }
          }
        } catch (Exception e) {
          logger.error("failed to load transferAssetContract, error'{}'", e);
        }
      }
    }

    // receipt
    if (Objects.nonNull(trxTrace) && Objects.nonNull(trxTrace.getReceipt())) {
      transactionLogTrigger.setEnergyFee(trxTrace.getReceipt().getEnergyFee());
      transactionLogTrigger.setOriginEnergyUsage(trxTrace.getReceipt().getOriginEnergyUsage());
      transactionLogTrigger.setEnergyUsageTotal(trxTrace.getReceipt().getEnergyUsageTotal());
      transactionLogTrigger.setNetUsage(trxTrace.getReceipt().getNetUsage());
      transactionLogTrigger.setNetFee(trxTrace.getReceipt().getNetFee());
      transactionLogTrigger.setEnergyUsage(trxTrace.getReceipt().getEnergyUsage());
    }

    // program result
    if (Objects.nonNull(trxTrace) && Objects.nonNull(trxTrace.getRuntime()) && Objects
        .nonNull(trxTrace.getRuntime().getResult())) {
      ProgramResult programResult = trxTrace.getRuntime().getResult();
      ByteString contractResult = ByteString.copyFrom(programResult.getHReturn());
      ByteString contractAddress = ByteString.copyFrom(programResult.getContractAddress());
      List<Map<String, Object>> triggerList = new ArrayList<Map<String, Object>>();
      for (ContractTrigger trigger : trxTrace.getRuntimeResult().getTriggerList()) {
        Map<String, Object> triggerCopy = new HashMap<String, Object>();
        triggerCopy.put("transactionId", trigger.getTransactionId());
        triggerCopy.put("callerAddress", trigger.getCallerAddress());
        triggerCopy.put("originAddress", trigger.getOriginAddress());
        triggerCopy.put("contractAddress", trigger.getContractAddress());
        triggerCopy.put("creatorAddress", trigger.getCreatorAddress());
        triggerCopy.put("rawData", trigger.getRawData());
        triggerList.add(triggerCopy);
      }
      transactionLogTrigger.setTriggerList(triggerList);
      transactionLogTrigger.setLogInfoList(programResult.getLogInfoList());
      if (Objects.nonNull(contractResult) && contractResult.size() > 0) {
        transactionLogTrigger.setContractResult(Hex.toHexString(contractResult.toByteArray()));
      }

      if (Objects.nonNull(contractAddress) && contractAddress.size() > 0) {
        transactionLogTrigger
            .setContractAddress(WalletUtil.encode58Check((contractAddress.toByteArray())));
      }

      // internal transaction
      transactionLogTrigger.setInternalTrananctionList(
          getInternalTransactionList(programResult.getInternalTransactions()));
    }
  }

  public void setLatestSolidifiedBlockNumber(long latestSolidifiedBlockNumber) {
    transactionLogTrigger.setLatestSolidifiedBlockNumber(latestSolidifiedBlockNumber);
  }

  private List<InternalTransactionPojo> getInternalTransactionList(
      List<InternalTransaction> internalTransactionList) {
    List<InternalTransactionPojo> pojoList = new ArrayList<>();

    internalTransactionList.forEach(internalTransaction -> {
      InternalTransactionPojo item = new InternalTransactionPojo();

      item.setHash(Hex.toHexString(internalTransaction.getHash()));
      item.setCallValue(internalTransaction.getValue());
      item.setTokenInfo(internalTransaction.getTokenInfo());
      item.setCaller_address(Hex.toHexString(internalTransaction.getSender()));
      item.setTransferTo_address(Hex.toHexString(internalTransaction.getTransferToAddress()));
      item.setData(Hex.toHexString(internalTransaction.getData()));
      item.setRejected(internalTransaction.isRejected());
      item.setNote(internalTransaction.getNote());

      pojoList.add(item);
    });

    return pojoList;
  }

  @Override
  public void processTrigger() {
    EventPluginLoader.getInstance().postTransactionTrigger(transactionLogTrigger);
  }
}
