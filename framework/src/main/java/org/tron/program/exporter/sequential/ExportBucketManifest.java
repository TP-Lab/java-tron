package org.tron.program.exporter.sequential;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
  private static final String CHECKPOINT_LOG_FILE_NAME = "checkpoints.log";
  private static final String PROPERTY_MANIFEST_VERSION = "manifestVersion";
  private static final String PROPERTY_CHECKPOINT_MODE = "checkpointMode";
  private static final String CURRENT_MANIFEST_VERSION = "2";
  private static final String CHECKPOINT_MODE_PREFIX_ONLY = "prefix-only";

  private final File workingDir;
  private final long startBlockNum;
  private final long endBlockNum;
  private final long bucketBlockCount;
  private final boolean prefixCheckpointCompatible;
  private final List<Bucket> buckets;
  private BufferedWriter checkpointWriter;

  private ExportBucketManifest(File workingDir, long startBlockNum, long endBlockNum,
      long bucketBlockCount, boolean prefixCheckpointCompatible, List<Bucket> buckets) {
    this.workingDir = workingDir;
    this.startBlockNum = startBlockNum;
    this.endBlockNum = endBlockNum;
    this.bucketBlockCount = bucketBlockCount;
    this.prefixCheckpointCompatible = prefixCheckpointCompatible;
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
        bucketBlockCount, true, buckets);
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
    boolean prefixCheckpointCompatible =
        CHECKPOINT_MODE_PREFIX_ONLY.equals(properties.getProperty(PROPERTY_CHECKPOINT_MODE))
            || CURRENT_MANIFEST_VERSION.equals(properties.getProperty(PROPERTY_MANIFEST_VERSION));

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

    applyCheckpointLog(workingDir, buckets);

    return new ExportBucketManifest(workingDir, startBlockNum, endBlockNum, bucketBlockCount,
        prefixCheckpointCompatible, buckets);
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

  public boolean supportsPrefixCheckpointResume() {
    return prefixCheckpointCompatible;
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
    closeCheckpointWriter();

    Properties properties = new Properties();
    properties.setProperty("startBlockNum", String.valueOf(startBlockNum));
    properties.setProperty("endBlockNum", String.valueOf(endBlockNum));
    properties.setProperty("bucketBlockCount", String.valueOf(bucketBlockCount));
    properties.setProperty(PROPERTY_MANIFEST_VERSION, CURRENT_MANIFEST_VERSION);
    properties.setProperty(PROPERTY_CHECKPOINT_MODE, CHECKPOINT_MODE_PREFIX_ONLY);

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

    resetCheckpointLog();
  }

  public void appendCheckpoint(Bucket bucket) throws IOException {
    ensureDirectory(workingDir);
    BufferedWriter writer = checkpointWriter();
    writer.write(bucket.getIndex() + "\t" + bucket.getNextExportBlockNum());
    writer.newLine();
    writer.flush();
  }

  @Override
  public void close() throws IOException {
    save();
    closeCheckpointWriter();
  }

  private static void ensureDirectory(File directory) throws IOException {
    if (!directory.exists() && !directory.mkdirs()) {
      throw new IOException("Could not create working directory: " + directory.getAbsolutePath());
    }
  }

  private static void applyCheckpointLog(File workingDir, List<Bucket> buckets) throws IOException {
    File checkpointLogFile = new File(workingDir, CHECKPOINT_LOG_FILE_NAME);
    if (!checkpointLogFile.exists() || checkpointLogFile.length() == 0) {
      return;
    }

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        new FileInputStream(checkpointLogFile), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.trim().isEmpty()) {
          continue;
        }

        String[] parts = line.split("\t");
        if (parts.length != 2) {
          throw new IOException("Invalid checkpoint log line: " + line);
        }

        int bucketIndex = Integer.parseInt(parts[0]);
        long nextExportBlockNum = Long.parseLong(parts[1]);
        if (bucketIndex < 0 || bucketIndex >= buckets.size()) {
          throw new IOException("Invalid checkpoint bucket index: " + bucketIndex);
        }

        Bucket bucket = buckets.get(bucketIndex);
        if (nextExportBlockNum <= bucket.getNextExportBlockNum()) {
          continue;
        }
        bucket.applyPersistedCheckpoint(nextExportBlockNum);
      }
    }
  }

  private void resetCheckpointLog() throws IOException {
    File checkpointLogFile = new File(workingDir, CHECKPOINT_LOG_FILE_NAME);
    if (!checkpointLogFile.exists()) {
      return;
    }

    try (OutputStream ignored = new FileOutputStream(checkpointLogFile, false)) {
      // Truncate the log after writing a full snapshot.
    }
  }

  private BufferedWriter checkpointWriter() throws IOException {
    if (checkpointWriter == null) {
      checkpointWriter = new BufferedWriter(new OutputStreamWriter(
          new FileOutputStream(new File(workingDir, CHECKPOINT_LOG_FILE_NAME), true),
          StandardCharsets.UTF_8));
    }
    return checkpointWriter;
  }

  private void closeCheckpointWriter() throws IOException {
    if (checkpointWriter == null) {
      return;
    }
    checkpointWriter.close();
    checkpointWriter = null;
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
    private long nextExportBlockNum;

    private Bucket(int index, long startBlockNum, long endBlockNum, String shardFileName) {
      this.index = index;
      this.startBlockNum = startBlockNum;
      this.endBlockNum = endBlockNum;
      this.shardFileName = shardFileName;
      this.nextExportBlockNum = startBlockNum;
    }

    static Bucket parse(String line) {
      String[] parts = line.split("\t");
      if (parts.length != 8 && parts.length != 9) {
        throw new IllegalArgumentException("Invalid bucket manifest line: " + line);
      }

      Bucket bucket = new Bucket(Integer.parseInt(parts[0]), Long.parseLong(parts[1]),
          Long.parseLong(parts[2]), parts[3]);
      bucket.prepared = Boolean.parseBoolean(parts[4]);
      bucket.exported = Boolean.parseBoolean(parts[5]);
      bucket.recordCount = Long.parseLong(parts[6]);
      bucket.serializedBytes = Long.parseLong(parts[7]);
      bucket.nextExportBlockNum = parts.length == 9
          ? Long.parseLong(parts[8])
          : (bucket.exported ? bucket.endBlockNum + 1 : bucket.startBlockNum);
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
          + serializedBytes + "\t"
          + nextExportBlockNum;
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

    public long getNextExportBlockNum() {
      return nextExportBlockNum;
    }

    public void markPrepared(long recordCount, long serializedBytes) {
      this.prepared = true;
      this.recordCount = recordCount;
      this.serializedBytes = serializedBytes;
      this.exported = false;
      this.nextExportBlockNum = startBlockNum;
    }

    public void markUnprepared() {
      this.prepared = false;
      this.exported = false;
      this.recordCount = 0;
      this.serializedBytes = 0;
      this.nextExportBlockNum = startBlockNum;
    }

    public boolean hasPartialCheckpoint() {
      return !exported && nextExportBlockNum > startBlockNum && nextExportBlockNum <= endBlockNum;
    }

    public void markBlockExported(long blockNum) {
      if (blockNum < nextExportBlockNum) {
        return;
      }
      this.nextExportBlockNum = blockNum + 1;
      if (nextExportBlockNum > endBlockNum) {
        markExported();
      }
    }

    public void markExported() {
      this.exported = true;
      this.nextExportBlockNum = endBlockNum + 1;
    }

    private void applyPersistedCheckpoint(long persistedNextExportBlockNum) {
      if (persistedNextExportBlockNum <= nextExportBlockNum) {
        return;
      }
      this.nextExportBlockNum = persistedNextExportBlockNum;
      this.exported = nextExportBlockNum > endBlockNum;
    }
  }
}
