package org.tron.core.store;

import com.google.protobuf.ByteString;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.BooleanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.utils.ByteArray;
import org.tron.core.capsule.TransactionInfoCapsule;
import org.tron.core.capsule.TransactionRetCapsule;
import org.tron.core.db.common.iterator.DBIterator;
import org.tron.core.db.TransactionStore;
import org.tron.core.db.TronStoreWithRevoking;
import org.tron.core.exception.BadItemException;
import org.tron.protos.Protocol;
import org.tron.protos.Protocol.TransactionInfo;

@Slf4j(topic = "DB")
@Component
public class TransactionRetStore extends TronStoreWithRevoking<TransactionRetCapsule> {

  @Autowired
  private TransactionStore transactionStore;

  @Autowired
  public TransactionRetStore(@Value("transactionRetStore") String dbName) {
    super(dbName);
  }

  @Override
  public void put(byte[] key, TransactionRetCapsule item) {
    if (BooleanUtils.toBoolean(CommonParameter.getInstance()
        .getStorage().getTransactionHistorySwitch())) {
      super.put(key, item);
    }
  }

  public TransactionInfoCapsule getTransactionInfo(byte[] key) throws BadItemException {
    long blockNumber = transactionStore.getBlockNumber(key);
    if (blockNumber == -1) {
      return null;
    }
    byte[] value = revokingDB.getUnchecked(ByteArray.fromLong(blockNumber));
    if (Objects.isNull(value)) {
      return null;
    }

    TransactionRetCapsule result = new TransactionRetCapsule(value);
    if (Objects.isNull(result.getInstance())) {
      return null;
    }

    ByteString id = ByteString.copyFrom(key);
    for (TransactionInfo transactionResultInfo : result.getInstance().getTransactioninfoList()) {
      if (transactionResultInfo.getId().equals(id)) {
        Protocol.ResourceReceipt receipt = transactionResultInfo.getReceipt();
        // If query a result with dirty origin usage in receipt, we just reset it.
        if (receipt.getEnergyUsageTotal() == 0 && receipt.getOriginEnergyUsage() > 0) {
          transactionResultInfo =
              transactionResultInfo.toBuilder()
                  .setReceipt(
                      receipt.toBuilder()
                          .clearOriginEnergyUsage()
                          .build())
                  .build();
        }
        return new TransactionInfoCapsule(transactionResultInfo);
      }
    }
    return null;
  }

  public TransactionRetCapsule getTransactionInfoByBlockNum(byte[] key) throws BadItemException {

    byte[] value = revokingDB.getUnchecked(key);
    if (Objects.isNull(value)) {
      return null;
    }

    return new TransactionRetCapsule(value);
  }

  /**
   * Batch range scan: returns TransactionRetCapsule for all blocks in [startBlock, endBlock].
   * Uses a direct iterator seek on the underlying DB and stops exactly at endBlock.
   *
   * <p>TransactionRetStore only persists blocks that actually contain transactions. If we scan by
   * "record count" instead of "block number upper bound", sparse ranges can over-read far beyond
   * endBlock and create significant I/O amplification.
   */
  public Map<Long, TransactionRetCapsule> getRange(long startBlock, long endBlock) {
    Map<Long, TransactionRetCapsule> result = new LinkedHashMap<>();
    if (endBlock < startBlock) {
      return result;
    }

    try (DBIterator iterator = (DBIterator) getDb().iterator()) {
      iterator.seek(ByteArray.fromLong(startBlock));
      while (iterator.valid()) {
        long blockNum = ByteArray.toLong(iterator.getKey());
        if (blockNum > endBlock) {
          break;
        }

        byte[] value = iterator.getValue();
        if (value != null) {
          try {
            result.put(blockNum, new TransactionRetCapsule(value));
          } catch (BadItemException e) {
            logger.warn("Skipping malformed TransactionRetCapsule for block {}: {}", blockNum,
                e.getMessage());
          }
        }
        iterator.next();
      }
    } catch (Exception e) {
      logger.warn("Failed to range scan TransactionRetStore [{}, {}]: {}",
          startBlock, endBlock, e.getMessage());
    }

    return result;
  }

}
