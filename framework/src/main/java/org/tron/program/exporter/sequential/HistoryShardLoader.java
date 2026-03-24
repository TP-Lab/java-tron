package org.tron.program.exporter.sequential;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.protos.Protocol.TransactionInfo;

public final class HistoryShardLoader {

  public BucketShardData load(File shardFile) throws IOException {
    if (!shardFile.exists() || shardFile.length() == 0) {
      return BucketShardData.empty();
    }

    Map<Long, Map<WrappedByteArray, TransactionInfo>> infosByBlock = new HashMap<>();
    long recordCount = 0;
    long payloadBytes = 0;

    try (DataInputStream inputStream = new DataInputStream(
        new BufferedInputStream(new FileInputStream(shardFile)))) {
      while (true) {
        try {
          long blockNum = inputStream.readLong();
          int txIdLength = inputStream.readInt();
          byte[] txId = new byte[txIdLength];
          inputStream.readFully(txId);
          int payloadLength = inputStream.readInt();
          byte[] payload = new byte[payloadLength];
          inputStream.readFully(payload);

          TransactionInfo transactionInfo = parseTransactionInfo(payload);
          Map<WrappedByteArray, TransactionInfo> perBlockMap = infosByBlock.get(blockNum);
          if (perBlockMap == null) {
            perBlockMap = new HashMap<>();
            infosByBlock.put(blockNum, perBlockMap);
          }
          perBlockMap.put(WrappedByteArray.of(txId), transactionInfo);
          recordCount++;
          payloadBytes += payloadLength;
        } catch (EOFException eof) {
          break;
        }
      }
    }

    return new BucketShardData(infosByBlock, recordCount, payloadBytes);
  }

  private static TransactionInfo parseTransactionInfo(byte[] payload) throws IOException {
    try {
      return TransactionInfo.parseFrom(payload);
    } catch (InvalidProtocolBufferException e) {
      throw new IOException("Failed to parse TransactionInfo payload", e);
    }
  }

  public static final class BucketShardData {
    private final Map<Long, Map<WrappedByteArray, TransactionInfo>> infosByBlock;
    private final long recordCount;
    private final long payloadBytes;

    private BucketShardData(Map<Long, Map<WrappedByteArray, TransactionInfo>> infosByBlock,
        long recordCount, long payloadBytes) {
      this.infosByBlock = infosByBlock;
      this.recordCount = recordCount;
      this.payloadBytes = payloadBytes;
    }

    static BucketShardData empty() {
      return new BucketShardData(new HashMap<Long, Map<WrappedByteArray, TransactionInfo>>(),
          0, 0);
    }

    public Map<WrappedByteArray, TransactionInfo> getBlockInfos(long blockNum) {
      Map<WrappedByteArray, TransactionInfo> infos = infosByBlock.get(blockNum);
      return infos == null ? Collections.<WrappedByteArray, TransactionInfo>emptyMap() : infos;
    }

    public Map<Long, Map<WrappedByteArray, TransactionInfo>> getInfosByBlock() {
      return infosByBlock;
    }

    public long getRecordCount() {
      return recordCount;
    }

    public long getPayloadBytes() {
      return payloadBytes;
    }
  }
}
