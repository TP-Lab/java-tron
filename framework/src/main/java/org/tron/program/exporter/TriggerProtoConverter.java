package org.tron.program.exporter;

import org.tron.common.logsfilter.trigger.InternalTransactionPojo;
import org.tron.common.logsfilter.trigger.LogPojo;
import org.tron.common.logsfilter.trigger.TransactionLogTrigger;
import org.tron.protos.TransactionLogTriggerProtos;

/**
 * TransactionLogTrigger POJO → Protobuf 转换器
 */
public class TriggerProtoConverter {

  public static TransactionLogTriggerProtos.TransactionLogTriggerPB convert(TransactionLogTrigger trigger) {
    TransactionLogTriggerProtos.TransactionLogTriggerPB.Builder builder =
        TransactionLogTriggerProtos.TransactionLogTriggerPB.newBuilder();

    // Trigger base
    builder.setTimeStamp(trigger.getTimeStamp());
    if (trigger.getTriggerName() != null) builder.setTriggerName(trigger.getTriggerName());

    // Transaction info
    if (trigger.getTransactionId() != null) builder.setTransactionId(trigger.getTransactionId());
    if (trigger.getBlockHash() != null) builder.setBlockHash(trigger.getBlockHash());
    builder.setBlockNumber(trigger.getBlockNumber());
    builder.setEnergyUsage(trigger.getEnergyUsage());
    builder.setEnergyFee(trigger.getEnergyFee());
    builder.setOriginEnergyUsage(trigger.getOriginEnergyUsage());
    builder.setEnergyUsageTotal(trigger.getEnergyUsageTotal());
    builder.setNetUsage(trigger.getNetUsage());
    builder.setNetFee(trigger.getNetFee());
    builder.setMemoFee(trigger.getMemoFee());
    builder.setMultiSignFee(trigger.getMultiSignFee());
    builder.setFee(trigger.getFee());

    // Contract
    if (trigger.getResult() != null) builder.setResult(trigger.getResult());
    if (trigger.getTxResult() != null) builder.setTxResult(trigger.getTxResult());
    if (trigger.getContractAddress() != null) builder.setContractAddress(trigger.getContractAddress());
    if (trigger.getContractType() != null) builder.setContractType(trigger.getContractType());
    builder.setFeeLimit(trigger.getFeeLimit());
    builder.setContractCallValue(trigger.getContractCallValue());
    if (trigger.getContractData() != null) builder.setContractData(trigger.getContractData());
    if (trigger.getContractResult() != null) builder.setContractResult(trigger.getContractResult());

    // Transfer
    if (trigger.getFromAddress() != null) builder.setFromAddress(trigger.getFromAddress());
    if (trigger.getToAddress() != null) builder.setToAddress(trigger.getToAddress());
    if (trigger.getAssetName() != null) builder.setAssetName(trigger.getAssetName());
    builder.setAssetAmount(trigger.getAssetAmount());
    builder.setLatestSolidifiedBlockNumber(trigger.getLatestSolidifiedBlockNumber());

    // Data & indexes
    if (trigger.getData() != null) builder.setData(trigger.getData());
    builder.setTransactionIndex(trigger.getTransactionIndex());
    builder.setCumulativeEnergyUsed(trigger.getCumulativeEnergyUsed());
    builder.setPreCumulativeLogCount(trigger.getPreCumulativeLogCount());
    builder.setEnergyUnitPrice(trigger.getEnergyUnitPrice());
    if (trigger.getTransactionDetail() != null) builder.setTransactionDetail(trigger.getTransactionDetail());

    // Signatures
    if (trigger.getSignatures() != null) {
      builder.addAllSignatures(trigger.getSignatures());
    }

    // Internal transactions
    if (trigger.getInternalTransactionList() != null) {
      for (InternalTransactionPojo pojo : trigger.getInternalTransactionList()) {
        builder.addInternalTransactionList(convertInternalTx(pojo));
      }
    }

    // Logs
    if (trigger.getLogList() != null) {
      for (LogPojo logPojo : trigger.getLogList()) {
        builder.addLogList(convertLog(logPojo));
      }
    }

    // ExtMap
    if (trigger.getExtMap() != null) {
      builder.putAllExtMap(trigger.getExtMap());
    }

    return builder.build();
  }

  private static TransactionLogTriggerProtos.InternalTransactionPojo convertInternalTx(InternalTransactionPojo pojo) {
    TransactionLogTriggerProtos.InternalTransactionPojo.Builder b =
        TransactionLogTriggerProtos.InternalTransactionPojo.newBuilder();
    if (pojo.getHash() != null) b.setHash(pojo.getHash());
    b.setCallValue(pojo.getCallValue());
    if (pojo.getTokenInfo() != null) b.putAllTokenInfo(pojo.getTokenInfo());
    if (pojo.getTransferTo_address() != null) b.setTransferToAddress(pojo.getTransferTo_address());
    if (pojo.getData() != null) b.setData(pojo.getData());
    if (pojo.getCaller_address() != null) b.setCallerAddress(pojo.getCaller_address());
    b.setRejected(pojo.isRejected());
    if (pojo.getNote() != null) b.setNote(pojo.getNote());
    if (pojo.getExtra() != null) b.setExtra(pojo.getExtra());
    return b.build();
  }

  private static TransactionLogTriggerProtos.LogPojo convertLog(LogPojo logPojo) {
    TransactionLogTriggerProtos.LogPojo.Builder b =
        TransactionLogTriggerProtos.LogPojo.newBuilder();
    if (logPojo.getAddress() != null) b.setAddress(logPojo.getAddress());
    if (logPojo.getBlockHash() != null) b.setBlockHash(logPojo.getBlockHash());
    b.setBlockNumber(logPojo.getBlockNumber());
    if (logPojo.getData() != null) b.setData(logPojo.getData());
    b.setLogIndex(logPojo.getLogIndex());
    if (logPojo.getTopicList() != null) b.addAllTopicList(logPojo.getTopicList());
    if (logPojo.getTransactionHash() != null) b.setTransactionHash(logPojo.getTransactionHash());
    b.setTransactionIndex(logPojo.getTransactionIndex());
    return b.build();
  }
}
