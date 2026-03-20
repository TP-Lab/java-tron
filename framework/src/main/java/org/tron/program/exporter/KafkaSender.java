package org.tron.program.exporter;

import com.google.common.util.concurrent.RateLimiter;
import java.io.Closeable;
import java.util.Properties;
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

  private KafkaProducer<String, String> stringProducer;
  private KafkaProducer<String, byte[]> bytesProducer;
  private RateLimiter rateLimiter;

  /**
   * 初始化 JSON 模式的 Kafka producer
   */
  public boolean initStringProducer(String brokers) {
    if (stringProducer != null) {
      return true;
    }
    try {
      Properties props = buildBaseProps(brokers);
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
      stringProducer = new KafkaProducer<>(props);
      logger.info("Kafka string producer initialized with brokers: {}", brokers);
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
    if (bytesProducer != null) {
      return true;
    }
    try {
      Properties props = buildBaseProps(brokers);
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
      bytesProducer = new KafkaProducer<>(props);
      logger.info("Kafka bytes producer initialized with brokers: {}", brokers);
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
      stringProducer.send(new ProducerRecord<>(topic, key, message), (metadata, exception) -> {
        if (exception != null) {
          logger.error("Kafka send failed: {}", exception.getMessage());
        }
      });
    } catch (Exception e) {
      logger.error("Kafka send error: {}", e.getMessage());
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
      bytesProducer.send(new ProducerRecord<>(topic, key, payload), (metadata, exception) -> {
        if (exception != null) {
          logger.error("Kafka proto send failed: {}", exception.getMessage());
        }
      });
    } catch (Exception e) {
      logger.error("Kafka proto send error: {}", e.getMessage());
    }
  }

  public boolean hasStringProducer() {
    return stringProducer != null;
  }

  public boolean hasBytesProducer() {
    return bytesProducer != null;
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

  private Properties buildBaseProps(String brokers) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "1");
    props.put(ProducerConfig.RETRIES_CONFIG, 3);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 131072);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 134217728);
    return props;
  }

  private void acquireRateLimit() {
    if (rateLimiter != null) {
      rateLimiter.acquire();
    }
  }
}
