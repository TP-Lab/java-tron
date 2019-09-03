package org.tron.common.logsfilter.trigger;

import lombok.Getter;
import lombok.Setter;
import org.tron.common.runtime.vm.LogInfo;

import java.util.List;
import java.util.Map;

public class TransactionLogTrigger extends Trigger {

  @Override
  public void setTimeStamp(long ts) {
    super.timeStamp = ts;
  }

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

  //contract
  @Getter
  @Setter
  private String result;

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
  private List<Map<String, Object>> triggerList;

  @Getter
  @Setter
  private List<LogInfo> logInfoList;

  @Getter
  @Setter
  private long latestSolidifiedBlockNumber;

  //internal transaction
  @Getter
  @Setter
  private List<InternalTransactionPojo> internalTrananctionList;

  public TransactionLogTrigger() {
    setTriggerName(Trigger.TRANSACTION_TRIGGER_NAME);
  }
}
