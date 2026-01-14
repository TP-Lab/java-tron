package org.tron.program;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.tron.common.application.TronApplicationContext;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.utils.ByteArray;
import org.tron.core.ChainBaseManager;
import org.tron.core.Constant;
import org.tron.core.Wallet;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.protos.Protocol.TransactionInfo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * KafkaExporter - Exports Tron Block and Transaction Log data to Kafka.
 *
 * <p>Usage:</p>
 * <pre>
 * java -cp &lt;classpath&gt; org.tron.program.KafkaExporter \
 *   --kafka &lt;kafka_brokers&gt; \
 *   --chain-id &lt;chain_id&gt; \
 *   [--from-block &lt;start_block&gt;] \
 *   [--to-block &lt;end_block&gt;] \
 *   [--config &lt;config_file&gt;] \
 *   [--output-directory &lt;data_dir&gt;]
 * </pre>
 *
 * <p>Example:</p>
 * <pre>
 * java -cp java-tron.jar org.tron.program.KafkaExporter \
 *   --kafka 127.0.0.1:9092 \
 *   --chain-id 728126428 \
 *   --from-block 0 \
 *   --to-block 1000 \
 *   -c config.conf \
 *   -d output-directory
 * </pre>
 */
@Slf4j(topic = "app")
public class KafkaExporter {

    // Schema ID 5: BlockData
    private static final String BLOCK_DATA_SCHEMA = "{\"type\":\"record\",\"name\":\"BlockData\",\"fields\":[{\"name\":\"chain_id\",\"type\":\"string\"},{\"name\":\"block_number\",\"type\":\"long\"},{\"name\":\"block_hash\",\"type\":\"string\"},{\"name\":\"log_count\",\"type\":\"long\"},{\"name\":\"timestamp\",\"type\":\"long\"}]}";
    private static final int BLOCK_DATA_SCHEMA_ID = 5;

    // Schema ID 4: LogJsonDataWithTopic
    private static final String LOG_DATA_SCHEMA = "{\"type\":\"record\",\"name\":\"LogJsonDataWithTopic\",\"fields\":[{\"name\":\"address\",\"type\":\"string\"},{\"name\":\"chain_id\",\"type\":\"string\"},{\"name\":\"topic0\",\"type\":\"string\"},{\"name\":\"topic1\",\"type\":\"string\"},{\"name\":\"topic2\",\"type\":\"string\"},{\"name\":\"topic3\",\"type\":\"string\"},{\"name\":\"data\",\"type\":\"string\"},{\"name\":\"block_hash\",\"type\":\"string\"},{\"name\":\"block_number\",\"type\":\"long\"},{\"name\":\"transaction_hash\",\"type\":\"string\"},{\"name\":\"transaction_index\",\"type\":\"long\"},{\"name\":\"log_index\",\"type\":\"long\"},{\"name\":\"removed\",\"type\":\"boolean\"}]}";
    private static final int LOG_DATA_SCHEMA_ID = 4;

    public static void main(String[] args) {
        ExporterConfig config = new ExporterConfig();
        JCommander.newBuilder().addObject(config).build().parse(args);

        if (config.help) {
            JCommander.newBuilder().addObject(config).build().usage();
            return;
        }

        try {
            run(config);
        } catch (Exception e) {
            log.error("Execution failed", e);
            System.exit(1);
        }
    }

    private static void run(ExporterConfig config) throws Exception {
        // Setup Tron Context (ReadOnly)
        setupTronContext(config);

        // Setup Kafka Producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBrokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1"); // RequireOne
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

        // Avro Schemas
        Schema blockSchema = new Schema.Parser().parse(BLOCK_DATA_SCHEMA);
        Schema logSchema = new Schema.Parser().parse(LOG_DATA_SCHEMA);

        // Get ChainBaseManager
        // Note: We use a static access to the context created in setupTronContext, or pass it.
        // Since we are inside the same process, we can rely on the spring context we create.
        
        // Initialize context
        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        beanFactory.setAllowCircularReferences(false);
        TronApplicationContext context = new TronApplicationContext(beanFactory);
        context.register(DefaultConfig.class);
        context.refresh();
        
        ChainBaseManager chainBaseManager = context.getBean(ChainBaseManager.class);
        Wallet wallet = context.getBean(Wallet.class);

        long from = config.fromBlock;
        long to = config.toBlock;
        if (to == 0) {
            to = chainBaseManager.getHeadBlockNum();
            log.info("No end block specified, using head: {}", to);
        }

        log.info("Starting processing from {} to {}", from, to);

        AsyncTracker tracker = new AsyncTracker();

        for (long num = from; num <= to; num++) {
            // Fail fast check
            if (tracker.hasError) {
                throw new RuntimeException("Kafka export failed, stopping execution.", tracker.lastException);
            }

            BlockCapsule block = chainBaseManager.getBlockByNum(num);
            if (block == null) {
                log.warn("Block {} not found", num);
                continue;
            }

            processBlock(block, config, producer, blockSchema, logSchema, tracker, wallet);

            if (num % 100 == 0) {
                log.info("Processed block {}", num);
            }
        }

        log.info("Waiting for pending messages...");
        producer.flush();
        tracker.waitUntilEmpty(); // Basic wait
        
        // Final check after flush
        if (tracker.hasError) {
            throw new RuntimeException("Kafka export failed during flush.", tracker.lastException);
        }

        producer.close();
        context.close();
        log.info("Done.");
    }

    private static void processBlock(BlockCapsule block, ExporterConfig config, KafkaProducer<String, byte[]> producer,
                                     Schema blockSchema, Schema logSchema, AsyncTracker tracker, Wallet wallet) throws IOException {
        String blockHash = block.getBlockId().toString();
        long blockNum = block.getNum();
        long timestamp = block.getTimeStamp();

        List<GenericRecord> logRecords = new ArrayList<>();
        long logIndexTotal = 0;

        List<TransactionCapsule> txs = block.getTransactions();
        for (int txIndex = 0; txIndex < txs.size(); txIndex++) {
            TransactionCapsule tx = txs.get(txIndex);
            String txHash = Hex.toHexString(tx.getTransactionId().getBytes());
            
            TransactionInfo info = wallet.getTransactionInfoById(tx.getTransactionId().getByteString());
            if (info == null) {
                // If the block exists but tx info is missing, it might be an issue.
                // However, for some lightweight nodes or unconfirmed txs it might happen.
                // Given we are iterating verified blocks, this warning is useful.
                log.warn("Transaction Info not found for tx: {} in block: {}", txHash, blockNum);
                continue; 
            }

            List<TransactionInfo.Log> logs = info.getLogList();
            for (TransactionInfo.Log lg : logs) {
                GenericRecord logRecord = new GenericData.Record(logSchema);
                logRecord.put("address", Hex.toHexString(lg.getAddress().toByteArray()));
                logRecord.put("chain_id", config.chainId);
                logRecord.put("data", Hex.toHexString(lg.getData().toByteArray()));
                logRecord.put("block_hash", blockHash);
                logRecord.put("block_number", blockNum);
                logRecord.put("transaction_hash", txHash);
                logRecord.put("transaction_index", (long) txIndex);
                logRecord.put("log_index", logIndexTotal++); // Block-scoped log index
                logRecord.put("removed", false);

                List<String> topics = new ArrayList<>();
                for (com.google.protobuf.ByteString topic : lg.getTopicsList()) {
                    topics.add(Hex.toHexString(topic.toByteArray()));
                }
                
                logRecord.put("topic0", topics.size() > 0 ? topics.get(0) : null);
                logRecord.put("topic1", topics.size() > 1 ? topics.get(1) : null);
                logRecord.put("topic2", topics.size() > 2 ? topics.get(2) : null);
                logRecord.put("topic3", topics.size() > 3 ? topics.get(3) : null);

                logRecords.add(logRecord);
            }
        }

        // Send Block Data
        GenericRecord blockRecord = new GenericData.Record(blockSchema);
        blockRecord.put("chain_id", config.chainId);
        blockRecord.put("block_number", blockNum);
        blockRecord.put("block_hash", blockHash);
        blockRecord.put("log_count", (long) logRecords.size());
        blockRecord.put("timestamp", timestamp);

        String blockKey = String.format("%s-%d-%s", config.chainId, blockNum, blockHash);
        byte[] blockPayload = serializeAvro(blockSchema, blockRecord, BLOCK_DATA_SCHEMA_ID);
        
        producer.send(new ProducerRecord<>(config.blockTopic, blockKey, blockPayload), tracker.callback);
        tracker.pending.incrementAndGet();

        // Send Logs
        for (GenericRecord lr : logRecords) {
            String logKey = String.format("%s-%d-%s-%d", config.chainId, blockNum, blockHash, lr.get("log_index"));
            byte[] logPayload = serializeAvro(logSchema, lr, LOG_DATA_SCHEMA_ID);
            producer.send(new ProducerRecord<>(config.logTopic, logKey, logPayload), tracker.callback);
            tracker.pending.incrementAndGet();
        }
    }

    private static byte[] serializeAvro(Schema schema, GenericRecord record, int schemaId) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0x00);
        out.write((schemaId >>> 24) & 0xFF);
        out.write((schemaId >>> 16) & 0xFF);
        out.write((schemaId >>> 8) & 0xFF);
        out.write((schemaId) & 0xFF);

        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        writer.write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private static void setupTronContext(ExporterConfig config) {
        // Disable unnecessary services
        CommonParameter.getInstance().setNeedToUpdateAsset(false);
        CommonParameter.getInstance().setP2pDisable(true);
        CommonParameter.getInstance().setRpcEnable(false);
        CommonParameter.getInstance().setEventSubscribe(false);
        
        System.setProperty("database.readonly", "true");
        
        // Setup args
        Args.setParam(new String[]{"-c", config.configFile, "-d", config.outputDirectory}, Constant.TESTNET_CONF);
    }

    static class AsyncTracker {
        AtomicLong pending = new AtomicLong(0);
        volatile boolean hasError = false;
        volatile Exception lastException = null;

        Callback callback = (metadata, exception) -> {
            pending.decrementAndGet();
            if (exception != null) {
                hasError = true;
                lastException = exception;
                log.error("Kafka send failed", exception);
            }
        };

        void waitUntilEmpty() {
            while (pending.get() > 0) {
                if (hasError) {
                    throw new RuntimeException("Error occurred while waiting for pending messages", lastException);
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    static class ExporterConfig {
        @Parameter(names = {"--kafka"}, description = "Kafka brokers", required = true)
        String kafkaBrokers;

        @Parameter(names = {"--block-topic"}, description = "Block Topic", required = false)
        String blockTopic = "tp.risingwave.evm.block";

        @Parameter(names = {"--log-topic"}, description = "Log Topic", required = false)
        String logTopic = "tp.risingwave.evm.log.data";

        @Parameter(names = {"--chain-id"}, description = "Chain ID", required = true)
        String chainId;

        @Parameter(names = {"--from-block"}, description = "Start Block")
        long fromBlock = 0;

        @Parameter(names = {"--to-block"}, description = "End Block")
        long toBlock = 0; // 0 means Head

        @Parameter(names = {"-c", "--config"}, description = "Config File", required = false)
        String configFile = "config.conf";

        @Parameter(names = {"-d", "--output-directory"}, description = "Data Directory", required = false)
        String outputDirectory = "output-directory";

        @Parameter(names = "--help", help = true)
        boolean help;
    }
}
