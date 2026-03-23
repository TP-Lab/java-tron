package org.tron.program.exporter.sequential;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public final class ExportBucketManifest implements Closeable {

  private static final String META_FILE_NAME = "manifest.properties";
  private static final String BUCKETS_FILE_NAME = "buckets.tsv";

  private final File workingDir;
  private final long startBlockNum;
  private final long endBlockNum;
  private final long bucketBlockCount;
  private final List<Bucket> buckets;

  private ExportBucketManifest(File workingDir, long startBlockNum, long endBlockNum,
      long bucketBlockCount, List<Bucket> buckets) {
    this.workingDir = workingDir;
    this.startBlockNum = startBlockNum;
    this.endBlockNum = endBlockNum;
    this.bucketBlockCount = bucketBlockCount;
    this.buckets = buckets;
  }

  public static ExportBucketManifest create(File workingDir, long startBlockNum, long endBlockNum,
      long bucketBlockCount) throws IOException {
    if (bucketBlockCount <= 0) {
      throw new IllegalArgumentException("bucketBlockCount must be > 0");
    }

    ensureDirectory(workingDir);
    List<Bucket> buckets = new ArrayList<>();
    long bucketIndex = 0;
    for (long currentStart = startBlockNum; currentStart <= endBlockNum;
        currentStart += bucketBlockCount) {
      long currentEnd = Math.min(currentStart + bucketBlockCount - 1, endBlockNum);
      buckets.add(new Bucket((int) bucketIndex, currentStart, currentEnd,
          String.format("history-%020d-%020d.bin", currentStart, currentEnd)));
      bucketIndex++;
    }

    ExportBucketManifest manifest = new ExportBucketManifest(workingDir, startBlockNum, endBlockNum,
        bucketBlockCount, buckets);
    manifest.save();
    return manifest;
  }

  public static ExportBucketManifest load(File workingDir) throws IOException {
    File metaFile = new File(workingDir, META_FILE_NAME);
    File bucketsFile = new File(workingDir, BUCKETS_FILE_NAME);
    if (!metaFile.exists() || !bucketsFile.exists()) {
      throw new IOException("Manifest files do not exist in " + workingDir.getAbsolutePath());
    }

    Properties properties = new Properties();
    try (FileInputStream inputStream = new FileInputStream(metaFile)) {
      properties.load(inputStream);
    }

    long startBlockNum = Long.parseLong(properties.getProperty("startBlockNum"));
    long endBlockNum = Long.parseLong(properties.getProperty("endBlockNum"));
    long bucketBlockCount = Long.parseLong(properties.getProperty("bucketBlockCount"));

    List<Bucket> buckets = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        new FileInputStream(bucketsFile), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!line.trim().isEmpty()) {
          buckets.add(Bucket.parse(line));
        }
      }
    }

    return new ExportBucketManifest(workingDir, startBlockNum, endBlockNum, bucketBlockCount,
        buckets);
  }

  public File getWorkingDir() {
    return workingDir;
  }

  public long getStartBlockNum() {
    return startBlockNum;
  }

  public long getEndBlockNum() {
    return endBlockNum;
  }

  public long getBucketBlockCount() {
    return bucketBlockCount;
  }

  public List<Bucket> getBuckets() {
    return Collections.unmodifiableList(buckets);
  }

  public Bucket findBucket(long blockNum) {
    if (blockNum < startBlockNum || blockNum > endBlockNum) {
      return null;
    }

    int index = (int) ((blockNum - startBlockNum) / bucketBlockCount);
    if (index < 0 || index >= buckets.size()) {
      return null;
    }
    return buckets.get(index);
  }

  public File resolveShardFile(Bucket bucket) {
    return new File(workingDir, bucket.getShardFileName());
  }

  public void save() throws IOException {
    ensureDirectory(workingDir);

    Properties properties = new Properties();
    properties.setProperty("startBlockNum", String.valueOf(startBlockNum));
    properties.setProperty("endBlockNum", String.valueOf(endBlockNum));
    properties.setProperty("bucketBlockCount", String.valueOf(bucketBlockCount));

    try (FileOutputStream outputStream = new FileOutputStream(new File(workingDir, META_FILE_NAME))) {
      properties.store(outputStream, "Transaction history sequential export manifest");
    }

    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(new File(workingDir, BUCKETS_FILE_NAME)), StandardCharsets.UTF_8))) {
      for (Bucket bucket : buckets) {
        writer.write(bucket.toLine());
        writer.newLine();
      }
    }
  }

  @Override
  public void close() throws IOException {
    save();
  }

  private static void ensureDirectory(File directory) throws IOException {
    if (!directory.exists() && !directory.mkdirs()) {
      throw new IOException("Could not create working directory: " + directory.getAbsolutePath());
    }
  }

  public static final class Bucket {

    private final int index;
    private final long startBlockNum;
    private final long endBlockNum;
    private final String shardFileName;
    private boolean prepared;
    private boolean exported;
    private long recordCount;
    private long serializedBytes;

    private Bucket(int index, long startBlockNum, long endBlockNum, String shardFileName) {
      this.index = index;
      this.startBlockNum = startBlockNum;
      this.endBlockNum = endBlockNum;
      this.shardFileName = shardFileName;
    }

    static Bucket parse(String line) {
      String[] parts = line.split("\t");
      if (parts.length != 8) {
        throw new IllegalArgumentException("Invalid bucket manifest line: " + line);
      }

      Bucket bucket = new Bucket(Integer.parseInt(parts[0]), Long.parseLong(parts[1]),
          Long.parseLong(parts[2]), parts[3]);
      bucket.prepared = Boolean.parseBoolean(parts[4]);
      bucket.exported = Boolean.parseBoolean(parts[5]);
      bucket.recordCount = Long.parseLong(parts[6]);
      bucket.serializedBytes = Long.parseLong(parts[7]);
      return bucket;
    }

    String toLine() {
      return index + "\t"
          + startBlockNum + "\t"
          + endBlockNum + "\t"
          + shardFileName + "\t"
          + prepared + "\t"
          + exported + "\t"
          + recordCount + "\t"
          + serializedBytes;
    }

    public int getIndex() {
      return index;
    }

    public long getStartBlockNum() {
      return startBlockNum;
    }

    public long getEndBlockNum() {
      return endBlockNum;
    }

    public String getShardFileName() {
      return shardFileName;
    }

    public boolean isPrepared() {
      return prepared;
    }

    public boolean isExported() {
      return exported;
    }

    public long getRecordCount() {
      return recordCount;
    }

    public long getSerializedBytes() {
      return serializedBytes;
    }

    public void markPrepared(long recordCount, long serializedBytes) {
      this.prepared = true;
      this.recordCount = recordCount;
      this.serializedBytes = serializedBytes;
      this.exported = false;
    }

    public void markUnprepared() {
      this.prepared = false;
      this.exported = false;
      this.recordCount = 0;
      this.serializedBytes = 0;
    }

    public void markExported() {
      this.exported = true;
    }
  }
}
