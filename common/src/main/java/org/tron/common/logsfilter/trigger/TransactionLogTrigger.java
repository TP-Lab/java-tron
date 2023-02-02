package org.tron.common.logsfilter.trigger;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

public class TransactionLogTrigger extends Trigger {

  @Getter
  @Setter
  private String transactionId;
  @Getter
  @Setter
  private String blockHash;
  @Getter
  @Setter
  private long blockNumber = -1;
  @Getter
  @Setter
  private long energyUsage;
  @Getter
  @Setter
  private long energyFee;
  @Getter
  @Setter
  private long originEnergyUsage;
  @Getter
  @Setter
  private long energyUsageTotal;
  @Getter
  @Setter
  private long netUsage;
  @Getter
  @Setter
  private long netFee;
  @Getter
  @Setter
  private long memoFee;
  @Getter
  @Setter
  private long multiSignFee;
  @Getter
  @Setter
  private long fee;
  //contract
  @Getter
  @Setter
  private String result;
  @Getter
  @Setter
  private String txResult;
  @Getter
  @Setter
  private String contractAddress;
  @Getter
  @Setter
  private String contractType;
  @Getter
  @Setter
  private long feeLimit;
  @Getter
  @Setter
  private long contractCallValue;
  @Getter
  @Setter
  private String contractData;
  @Getter
  @Setter
  private String contractResult;
  // transfer contract
  @Getter
  @Setter
  private String fromAddress;
  @Getter
  @Setter
  private String toAddress;
  @Getter
  @Setter
  private String assetName;
  @Getter
  @Setter
  private long assetAmount;
  @Getter
  @Setter
  private long latestSolidifiedBlockNumber;
  //internal transaction
  @Getter
  @Setter
  private List<InternalTransactionPojo> internalTransactionList;
  @Getter
  @Setter
  private String data;
  @Getter
  @Setter
  private int transactionIndex;
  @Getter
  @Setter
  private long cumulativeEnergyUsed;
  @Getter
  @Setter
  private long preCumulativeLogCount;
  @Getter
  @Setter
  private List<LogPojo> logList;
  @Getter
  @Setter
  private long energyUnitPrice;

  public TransactionLogTrigger() {
    setTriggerName(Trigger.TRANSACTION_TRIGGER_NAME);
  }

  @Override
  public void setTimeStamp(long ts) {
    super.timeStamp = ts;
  }
}
