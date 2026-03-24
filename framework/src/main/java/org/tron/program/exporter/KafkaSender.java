package org.tron.program.exporter;

import com.google.common.util.concurrent.RateLimiter;
import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Kafka 发送封装 — 支持 JSON (String) 和 Protobuf (byte[]) 两种模式
 */
@Slf4j(topic = "app")
public class KafkaSender implements Closeable {

  private static final ProducerConfigProfile DEFAULT_STRING_PROFILE = ProducerConfigProfile.builder()
      .compressionType("gzip")
      .batchSizeBytes(131072)
      .lingerMs(20)
      .bufferMemoryBytes(134217728L)
      .acks("1")
      .callbacksEnabled(true)
      .build();

  private static final ProducerConfigProfile DEFAULT_BYTES_PROFILE = ProducerConfigProfile.builder()
      .compressionType("lz4")
      .batchSizeBytes(131072)
      .lingerMs(20)
      .bufferMemoryBytes(134217728L)
      .acks("1")
      .callbacksEnabled(true)
      .build();

  private KafkaProducer<String, String> stringProducer;
  private KafkaProducer<String, byte[]> bytesProducer;
  private RateLimiter rateLimiter;
  private ProducerConfigProfile stringProfile = DEFAULT_STRING_PROFILE;
  private ProducerConfigProfile bytesProfile = DEFAULT_BYTES_PROFILE;
  private final LongAdder stringMessagesQueued = new LongAdder();
  private final LongAdder bytesMessagesQueued = new LongAdder();
  private final LongAdder stringMessagesAcked = new LongAdder();
  private final LongAdder bytesMessagesAcked = new LongAdder();
  private final LongAdder asyncFailureCount = new LongAdder();
  private final AtomicReference<RuntimeException> firstAsyncException = new AtomicReference<>();

  /**
   * 初始化 JSON 模式的 Kafka producer
   */
  public boolean initStringProducer(String brokers) {
    return initStringProducer(brokers, DEFAULT_STRING_PROFILE);
  }

  public boolean initStringProducer(String brokers, ProducerConfigProfile profile) {
    if (stringProducer != null) {
      return true;
    }
    try {
      stringProfile = profile == null ? DEFAULT_STRING_PROFILE : profile;
      Properties props = buildBaseProps(brokers, stringProfile);
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      stringProducer = new KafkaProducer<>(props);
      logger.info("Kafka string producer initialized with brokers: {}, compression={}, "
              + "batch={}B, linger={}ms, buffer={}B, acks={}, callbacks={}",
          brokers, stringProfile.getCompressionType(), stringProfile.getBatchSizeBytes(),
          stringProfile.getLingerMs(), stringProfile.getBufferMemoryBytes(),
          stringProfile.getAcks(), stringProfile.isCallbacksEnabled());
      return true;
    } catch (Exception e) {
      logger.error("Failed to initialize Kafka string producer: {}", e.getMessage(), e);
      stringProducer = null;
      return false;
    }
  }

  /**
   * 初始化 Protobuf 模式的 Kafka producer
   */
  public boolean initBytesProducer(String brokers) {
    return initBytesProducer(brokers, DEFAULT_BYTES_PROFILE);
  }

  public boolean initBytesProducer(String brokers, ProducerConfigProfile profile) {
    if (bytesProducer != null) {
      return true;
    }
    try {
      bytesProfile = profile == null ? DEFAULT_BYTES_PROFILE : profile;
      Properties props = buildBaseProps(brokers, bytesProfile);
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      bytesProducer = new KafkaProducer<>(props);
      logger.info("Kafka bytes producer initialized with brokers: {}, compression={}, "
              + "batch={}B, linger={}ms, buffer={}B, acks={}, callbacks={}",
          brokers, bytesProfile.getCompressionType(), bytesProfile.getBatchSizeBytes(),
          bytesProfile.getLingerMs(), bytesProfile.getBufferMemoryBytes(),
          bytesProfile.getAcks(), bytesProfile.isCallbacksEnabled());
      return true;
    } catch (Exception e) {
      logger.error("Failed to initialize Kafka bytes producer: {}", e.getMessage(), e);
      bytesProducer = null;
      return false;
    }
  }

  public void setRateLimit(int messagesPerSecond) {
    if (messagesPerSecond > 0) {
      rateLimiter = RateLimiter.create(messagesPerSecond);
      logger.info("Kafka rate limit set to: {} messages/second", messagesPerSecond);
    }
  }

  /**
   * 发送 JSON 字符串消息
   */
  public void send(String topic, String key, String message) {
    if (stringProducer == null) {
      return;
    }
    acquireRateLimit();
    try {
      ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
      stringProducer.send(record, (metadata, exception) ->
          handleSendCallback("string", exception, stringProfile.isCallbacksEnabled(),
              stringMessagesAcked));
      stringMessagesQueued.increment();
    } catch (Exception e) {
      recordSendFailure("string", e, true);
      throw new IllegalStateException("Kafka string send failed immediately", e);
    }
  }

  /**
   * 发送 protobuf binary 消息
   */
  public void sendBytes(String topic, String key, byte[] payload) {
    if (bytesProducer == null) {
      return;
    }
    acquireRateLimit();
    try {
      ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, payload);
      bytesProducer.send(record, (metadata, exception) ->
          handleSendCallback("bytes", exception, bytesProfile.isCallbacksEnabled(),
              bytesMessagesAcked));
      bytesMessagesQueued.increment();
    } catch (Exception e) {
      recordSendFailure("bytes", e, true);
      throw new IllegalStateException("Kafka bytes send failed immediately", e);
    }
  }

  public boolean hasStringProducer() {
    return stringProducer != null;
  }

  public boolean hasBytesProducer() {
    return bytesProducer != null;
  }

  public void flushAndVerify() {
    flushProducer(stringProducer);
    flushProducer(bytesProducer);

    RuntimeException asyncException = firstAsyncException.get();
    if (asyncException != null) {
      throw new IllegalStateException("Kafka async send failed. asyncFailures="
          + asyncFailureCount.sum(), asyncException);
    }
  }

  @Override
  public void close() {
    if (stringProducer != null) {
      try {
        stringProducer.flush();
        stringProducer.close();
        logger.info("Kafka string producer closed");
      } catch (Exception e) {
        logger.error("Error closing Kafka string producer: {}", e.getMessage(), e);
      } finally {
        stringProducer = null;
      }
    }
    if (bytesProducer != null) {
      try {
        bytesProducer.flush();
        bytesProducer.close();
        logger.info("Kafka bytes producer closed");
      } catch (Exception e) {
        logger.error("Error closing Kafka bytes producer: {}", e.getMessage(), e);
      } finally {
        bytesProducer = null;
      }
    }
  }

  private Properties buildBaseProps(String brokers, ProducerConfigProfile profile) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, profile.getAcks());
    props.put(ProducerConfig.RETRIES_CONFIG, 3);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, profile.getBatchSizeBytes());
    props.put(ProducerConfig.LINGER_MS_CONFIG, profile.getLingerMs());
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, profile.getBufferMemoryBytes());
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, profile.getCompressionType());
    return props;
  }

  private void acquireRateLimit() {
    if (rateLimiter != null) {
      rateLimiter.acquire();
    }
  }

  private void flushProducer(KafkaProducer<?, ?> producer) {
    if (producer == null) {
      return;
    }
    producer.flush();
  }

  private void handleSendCallback(String producerType, Exception exception,
      boolean logFailure, LongAdder ackCounter) {
    if (exception == null) {
      ackCounter.increment();
      return;
    }

    recordSendFailure(producerType, exception, logFailure);
  }

  private void recordSendFailure(String producerType, Exception exception, boolean logFailure) {
    asyncFailureCount.increment();
    RuntimeException wrapped = new RuntimeException(
        "Kafka " + producerType + " send failed: " + exception.getMessage(), exception);
    firstAsyncException.compareAndSet(null, wrapped);
    if (logFailure) {
      logger.error("Kafka {} send failed: {}", producerType, exception.getMessage(), exception);
    }
  }

  public static final class ProducerConfigProfile {
    private final String compressionType;
    private final int batchSizeBytes;
    private final int lingerMs;
    private final long bufferMemoryBytes;
    private final String acks;
    private final boolean callbacksEnabled;

    private ProducerConfigProfile(Builder builder) {
      this.compressionType = builder.compressionType;
      this.batchSizeBytes = builder.batchSizeBytes;
      this.lingerMs = builder.lingerMs;
      this.bufferMemoryBytes = builder.bufferMemoryBytes;
      this.acks = builder.acks;
      this.callbacksEnabled = builder.callbacksEnabled;
    }

    public static Builder builder() {
      return new Builder();
    }

    public String getCompressionType() {
      return compressionType;
    }

    public int getBatchSizeBytes() {
      return batchSizeBytes;
    }

    public int getLingerMs() {
      return lingerMs;
    }

    public long getBufferMemoryBytes() {
      return bufferMemoryBytes;
    }

    public String getAcks() {
      return acks;
    }

    public boolean isCallbacksEnabled() {
      return callbacksEnabled;
    }

    public static final class Builder {
      private String compressionType = "lz4";
      private int batchSizeBytes = 131072;
      private int lingerMs = 20;
      private long bufferMemoryBytes = 134217728L;
      private String acks = "1";
      private boolean callbacksEnabled = true;

      private Builder() {
      }

      public Builder compressionType(String compressionType) {
        this.compressionType = compressionType;
        return this;
      }

      public Builder batchSizeBytes(int batchSizeBytes) {
        this.batchSizeBytes = batchSizeBytes;
        return this;
      }

      public Builder lingerMs(int lingerMs) {
        this.lingerMs = lingerMs;
        return this;
      }

      public Builder bufferMemoryBytes(long bufferMemoryBytes) {
        this.bufferMemoryBytes = bufferMemoryBytes;
        return this;
      }

      public Builder acks(String acks) {
        this.acks = acks;
        return this;
      }

      public Builder callbacksEnabled(boolean callbacksEnabled) {
        this.callbacksEnabled = callbacksEnabled;
        return this;
      }

      public ProducerConfigProfile build() {
        return new ProducerConfigProfile(this);
      }
    }
  }
}
