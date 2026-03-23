package org.tron.program.exporter.sequential;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

  private static final class BucketOutput {
    private final DataOutputStream stream;
    private long recordCount;
    private long serializedBytes;

    private BucketOutput(DataOutputStream stream) {
      this.stream = stream;
    }
  }
}
