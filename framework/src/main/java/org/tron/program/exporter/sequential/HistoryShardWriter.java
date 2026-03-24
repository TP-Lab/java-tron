package org.tron.program.exporter.sequential;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.protos.Protocol.TransactionInfo;

public final class HistoryShardWriter implements Closeable {

  private final ExportBucketManifest manifest;
  private final Map<Integer, BucketOutput> outputs = new HashMap<>();

  public HistoryShardWriter(ExportBucketManifest manifest) {
    this.manifest = manifest;
  }

  public boolean append(long blockNum, byte[] txId, byte[] payload) throws IOException {
    ExportBucketManifest.Bucket bucket = manifest.findBucket(blockNum);
    if (bucket == null) {
      return false;
    }

    BucketOutput output = getOrCreateOutput(bucket);
    output.stream.writeLong(blockNum);
    output.stream.writeInt(txId.length);
    output.stream.write(txId);
    output.stream.writeInt(payload.length);
    output.stream.write(payload);
    output.recordCount++;
    output.serializedBytes += txId.length + payload.length + 16L;
    return true;
  }

  @Override
  public void close() throws IOException {
    IOException closeException = null;

    for (ExportBucketManifest.Bucket bucket : manifest.getBuckets()) {
      BucketOutput output = outputs.get(bucket.getIndex());
      if (output == null) {
        deleteIfExists(manifest.resolveShardFile(bucket));
        bucket.markPrepared(0, 0);
        continue;
      }

      try {
        output.stream.flush();
        output.stream.close();
        bucket.markPrepared(output.recordCount, output.serializedBytes);
      } catch (IOException e) {
        closeException = e;
      }
    }

    manifest.save();

    if (closeException != null) {
      throw closeException;
    }
  }

  private BucketOutput getOrCreateOutput(ExportBucketManifest.Bucket bucket) throws IOException {
    BucketOutput existing = outputs.get(bucket.getIndex());
    if (existing != null) {
      return existing;
    }

    File shardFile = manifest.resolveShardFile(bucket);
    DataOutputStream stream = new DataOutputStream(new BufferedOutputStream(
        new FileOutputStream(shardFile, false)));
    BucketOutput output = new BucketOutput(stream);
    outputs.put(bucket.getIndex(), output);
    return output;
  }

  public static ShardStats rewriteShardFile(File shardFile,
      Map<Long, Map<WrappedByteArray, TransactionInfo>> infosByBlock) throws IOException {
    if (infosByBlock == null || infosByBlock.isEmpty()) {
      deleteIfExists(shardFile);
      return new ShardStats(0, 0);
    }

    long recordCount = 0;
    long serializedBytes = 0;
    List<Long> blockNums = new ArrayList<>(infosByBlock.keySet());
    Collections.sort(blockNums);

    try (DataOutputStream stream = new DataOutputStream(new BufferedOutputStream(
        new FileOutputStream(shardFile, false)))) {
      for (Long blockNum : blockNums) {
        Map<WrappedByteArray, TransactionInfo> blockInfos = infosByBlock.get(blockNum);
        if (blockInfos == null || blockInfos.isEmpty()) {
          continue;
        }

        for (Map.Entry<WrappedByteArray, TransactionInfo> entry : blockInfos.entrySet()) {
          byte[] txId = entry.getKey().getBytes();
          byte[] payload = entry.getValue().toByteArray();
          stream.writeLong(blockNum);
          stream.writeInt(txId.length);
          stream.write(txId);
          stream.writeInt(payload.length);
          stream.write(payload);
          recordCount++;
          serializedBytes += txId.length + payload.length + 16L;
        }
      }
      stream.flush();
    }

    return new ShardStats(recordCount, serializedBytes);
  }

  private static void deleteIfExists(File shardFile) throws IOException {
    if (shardFile.exists() && !shardFile.delete()) {
      throw new IOException("Could not delete shard file: " + shardFile.getAbsolutePath());
    }
  }

  private static final class BucketOutput {
    private final DataOutputStream stream;
    private long recordCount;
    private long serializedBytes;

    private BucketOutput(DataOutputStream stream) {
      this.stream = stream;
    }
  }

  public static final class ShardStats {
    private final long recordCount;
    private final long serializedBytes;

    public ShardStats(long recordCount, long serializedBytes) {
      this.recordCount = recordCount;
      this.serializedBytes = serializedBytes;
    }

    public long getRecordCount() {
      return recordCount;
    }

    public long getSerializedBytes() {
      return serializedBytes;
    }
  }
}
