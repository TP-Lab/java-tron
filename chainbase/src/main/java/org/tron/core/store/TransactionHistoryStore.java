package org.tron.core.store;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.tron.common.parameter.CommonParameter;
import org.tron.core.capsule.TransactionInfoCapsule;
import org.tron.core.db.TronStoreWithRevoking;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.exception.BadItemException;

@Component
@Slf4j(topic = "DB")
public class TransactionHistoryStore extends TronStoreWithRevoking<TransactionInfoCapsule> {

  @Autowired
  public TransactionHistoryStore(@Value("transactionHistoryStore") String dbName) {
    super(dbName);
  }

  @Override
  public TransactionInfoCapsule get(byte[] key) throws BadItemException {
    byte[] value = revokingDB.getUnchecked(key);
    return ArrayUtils.isEmpty(value) ? null : new TransactionInfoCapsule(value);
  }

  public Map<WrappedByteArray, TransactionInfoCapsule> getUnchecked(List<byte[]> keys) {
    if (keys == null || keys.isEmpty()) {
      return Collections.emptyMap();
    }

    if (getDb() instanceof org.tron.core.db2.common.RocksDB) {
      try {
        Map<byte[], byte[]> values = ((org.tron.core.db2.common.RocksDB) getDb())
            .getDb()
            .getDatabase()
            .multiGet(keys);
        Map<WrappedByteArray, TransactionInfoCapsule> result = new HashMap<>(values.size());
        for (Map.Entry<byte[], byte[]> entry : values.entrySet()) {
          if (!ArrayUtils.isEmpty(entry.getValue())) {
            result.put(WrappedByteArray.of(entry.getKey()),
                new TransactionInfoCapsule(entry.getValue()));
          }
        }
        return result;
      } catch (RocksDBException | BadItemException e) {
        log.debug("TransactionHistoryStore multiGet failed, fallback to point lookup: {}",
            e.getMessage());
      }
    }

    Map<WrappedByteArray, TransactionInfoCapsule> result = new HashMap<>(keys.size());
    for (byte[] key : keys) {
      TransactionInfoCapsule capsule = getUnchecked(key);
      if (capsule != null) {
        result.put(WrappedByteArray.of(key), capsule);
      }
    }
    return result;
  }

  @Override
  public void put(byte[] key, TransactionInfoCapsule item) {
    if (BooleanUtils.toBoolean(CommonParameter.getInstance()
        .getStorage().getTransactionHistorySwitch())) {
      super.put(key, item);
    }
  }
}
