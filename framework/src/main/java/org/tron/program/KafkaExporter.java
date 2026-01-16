package org.tron.program;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
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
 *   [--config &lt;config_file&gt;]
 * </pre>
 *
 * <p>Example:</p>
 * <pre>
 * java -cp java-tron.jar org.tron.program.KafkaExporter \
 *   --kafka 127.0.0.1:9092 \
 *   --chain-id 728126428 \
 *   --from-block 0 \
 *   --to-block 1000 \
 *   -c config.conf
 * </pre>
 */
public class KafkaExporter {
    private static final Logger log = LoggerFactory.getLogger("app");

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

        logDatabaseInfo(chainBaseManager, from, to);
        log.info("Starting processing from {} to {}", from, to);

        AsyncTracker tracker = new AsyncTracker();
        ExecutorService executor = Executors.newFixedThreadPool(config.threads);
        log.info("Initialized thread pool with {} threads", config.threads);

        // Preloading Buffer
        BlockingQueue<BlockTask> blockQueue = new LinkedBlockingQueue<>(50);
        
        final long finalFrom = from;
        final long finalTo = to;
        
        Thread preloader = new Thread(() -> {
            log.info("Preloader started.");
            for (long num = finalFrom; num <= finalTo; num++) {
                if (tracker.hasError || Thread.currentThread().isInterrupted()) break;
                try {
                    BlockCapsule block = chainBaseManager.getBlockByNum(num);
                    blockQueue.put(new BlockTask(num, block));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Error fetching block " + num, e);
                    // Insert a null-block task or handle error? 
                    // For now, if fetch fails, we might just put a task with null block or let main loop handle it.
                    // But simpler to just break? Or continue?
                    // Let's rely on fail-fast if serious error.
                }
            }
            log.info("Preloader finished.");
        }, "BlockPreloader");
        preloader.start();

        try {

        long startTime = System.currentTimeMillis();
        long totalTxs = 0;
        long totalLogs = 0;

        for (long num = from; num <= to; num++) {
            // Fail fast check
            if (tracker.hasError) {
                throw new RuntimeException("Kafka export failed, stopping execution.", tracker.lastException);
            }

            BlockTask task = null;
            try {
                task = blockQueue.take();
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               throw new RuntimeException("Main thread interrupted while waiting for block", e);
            }

            BlockCapsule block = task.block;
            if (block == null) {
                log.warn("Block {} not found", task.blockNum);
                continue;
            }

            int logsCount = processBlock(block, config, producer, blockSchema, logSchema, tracker, wallet, executor);
            
            totalTxs += block.getTransactions().size();
            totalLogs += logsCount;

            if (num % 100 == 0) {
                long elapsed = System.currentTimeMillis() - startTime;
                if (elapsed > 0) {
                   double blocksPerSec = (double)(num - from + 1) * 1000 / elapsed;
                   double txsPerSec = (double) totalTxs * 1000 / elapsed;
                   log.info("Processed block {} | Speed: {:.2f} blks/s, {:.2f} txs/s | Queue: {}/50 | Elapsed: {}s | Total Txs: {} | Total Logs: {}", 
                           num, blocksPerSec, txsPerSec, blockQueue.size(), elapsed / 1000, totalTxs, totalLogs);
                }
            }
        }

        log.info("Waiting for pending messages...");
        producer.flush();
        tracker.waitUntilEmpty(); // Basic wait
        
        // Final check after flush
        if (tracker.hasError) {
            throw new RuntimeException("Kafka export failed during flush.", tracker.lastException);
        }

        } finally {
            preloader.interrupt(); 
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }

        producer.close();
        context.close();
        log.info("Done.");
    }

    private static int processBlock(BlockCapsule block, ExporterConfig config, KafkaProducer<String, byte[]> producer,
                                     Schema blockSchema, Schema logSchema, AsyncTracker tracker, Wallet wallet, ExecutorService executor) throws IOException {
        String blockHash = block.getBlockId().toString();
        long blockNum = block.getNum();
        long timestamp = block.getTimeStamp();

        List<GenericRecord> logRecords = new ArrayList<>();
        long logIndexTotal = 0;

        List<TransactionCapsule> txs = block.getTransactions();
        List<Future<List<GenericRecord>>> futures = new ArrayList<>(txs.size());

        for (int i = 0; i < txs.size(); i++) {
            final int txIndex = i;
            final TransactionCapsule tx = txs.get(i);
            
            futures.add(executor.submit(() -> {
                List<GenericRecord> txLogs = new ArrayList<>();
                String txHash = Hex.toHexString(tx.getTransactionId().getBytes());
                
                // DB Read inside worker thread
                TransactionInfo info = wallet.getTransactionInfoById(tx.getTransactionId().getByteString());
                if (info == null) {
                    log.warn("Transaction Info not found for tx: {} in block: {}", txHash, blockNum);
                    return txLogs; 
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
                    // log_index set later
                    logRecord.put("removed", false);

                    List<String> topics = new ArrayList<>();
                    for (com.google.protobuf.ByteString topic : lg.getTopicsList()) {
                        topics.add(Hex.toHexString(topic.toByteArray()));
                    }
                    
                    logRecord.put("topic0", topics.size() > 0 ? topics.get(0) : "");
                    logRecord.put("topic1", topics.size() > 1 ? topics.get(1) : "");
                    logRecord.put("topic2", topics.size() > 2 ? topics.get(2) : "");
                    logRecord.put("topic3", topics.size() > 3 ? topics.get(3) : "");

                    txLogs.add(logRecord);
                }
                return txLogs;
            }));
        }

        // Collect results in order
        for (Future<List<GenericRecord>> future : futures) {
            try {
                List<GenericRecord> txLogs = future.get();
                for (GenericRecord rec : txLogs) {
                    rec.put("log_index", logIndexTotal++);
                    logRecords.add(rec);
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException("Error processing transaction", e);
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
        return logRecords.size();
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

    private static void logDatabaseInfo(ChainBaseManager chainBaseManager, long from, long to) {
        String configFile = Args.getInstance().getShellConfFileName();
        if (StringUtils.isBlank(configFile)) {
            log.info("Config file: <default>");
        } else {
            File configPath = new File(configFile);
            log.info("Config file: {} (exists: {}, absolute: {})",
                    configFile, configPath.exists(), configPath.getAbsolutePath());
        }

        String outputDirectory = Args.getInstance().getOutputDirectory();
        log.info("Output directory: {}", outputDirectory);
        if (Args.getInstance().getStorage() != null) {
            String dbDirectory = Args.getInstance().getStorage().getDbDirectory();
            String indexDirectory = Args.getInstance().getStorage().getIndexDirectory();
            log.info("Storage db directory: {}", dbDirectory);
            log.info("Storage index directory: {}", indexDirectory);
            log.info("Storage db engine: {}", Args.getInstance().getStorage().getDbEngine());
            log.info("Resolved db path: {}", resolveStoragePath(outputDirectory, dbDirectory));
            log.info("Resolved index path: {}", resolveStoragePath(outputDirectory, indexDirectory));
        } else {
            log.warn("Storage config not initialized.");
        }

        long lowestBlockNum = -1;
        long latestBlockNum = -1;
        try {
            lowestBlockNum = chainBaseManager.getLowestBlockNum();
        } catch (Exception e) {
            log.warn("Failed to get lowest block number: {}", e.getMessage());
        }

        try {
            latestBlockNum = chainBaseManager.getDynamicPropertiesStore().getLatestBlockHeaderNumber();
        } catch (Exception e) {
            log.warn("Failed to get latest block number: {}", e.getMessage());
        }

        if (lowestBlockNum >= 0 || latestBlockNum >= 0) {
            log.info("Database block range: {} to {}", lowestBlockNum, latestBlockNum);
        } else {
            log.warn("Database block range unavailable.");
        }

        if (lowestBlockNum >= 0 && latestBlockNum >= 0 && (from < lowestBlockNum || to > latestBlockNum)) {
            log.warn("Requested block range {} to {} outside database range {} to {}", from, to, lowestBlockNum, latestBlockNum);
        }
    }

    private static void setupTronContext(ExporterConfig config) {
        // Disable unnecessary services for read-only exporter mode
        CommonParameter parameter = CommonParameter.getInstance();
        parameter.setNeedToUpdateAsset(false);
        parameter.setP2pDisable(true);
        parameter.setRpcEnable(false);
        parameter.setRpcSolidityEnable(false);
        parameter.setRpcPBFTEnable(false);
        parameter.setFullNodeHttpEnable(false);
        parameter.setSolidityNodeHttpEnable(false);
        parameter.setPBFTHttpEnable(false);
        parameter.setJsonRpcHttpFullNodeEnable(false);
        parameter.setJsonRpcHttpSolidityNodeEnable(false);
        parameter.setJsonRpcHttpPBFTNodeEnable(false);
        parameter.setEventSubscribe(false);
        parameter.setNodeMetricsEnable(false);
        parameter.setMetricsStorageEnable(false);
        parameter.setMetricsPrometheusEnable(false);

        System.setProperty("database.readonly", "true");
        System.setProperty("storage.readonly", "true");
        
        // Setup args
        Args.setParam(new String[]{"-c", config.configFile}, Constant.TESTNET_CONF);
        normalizeStorageDirectories();

        if (Args.getInstance().getStorage() != null) {
            Args.getInstance().getStorage().setDbSync(false);
            Args.getInstance().getStorage().setMaxFlushCount(0);

            // Optimize Cache for Random Reads (which processBlock heavily relies on)
            // Increase BlockCache size if possible
            if (Args.getInstance().getStorage().getPropertyMap() != null) {
                Args.getInstance().getStorage().getPropertyMap().values().forEach(prop -> {
                    if (prop.getDbOptions() != null) {
                        // Increase cache size to 512MB (since machine has 60GB RAM)
                        // With ~20 DB instances, this consumes ~10GB of off-heap memory
                        prop.getDbOptions().cacheSize(512 * 1024 * 1024L); 
                    }
                });
                log.info("Optimized RocksDB cache size to 512MB per db");
            }
        }
    }

    private static void normalizeStorageDirectories() {
        if (Args.getInstance().getStorage() == null) {
            return;
        }

        String dbDirectory = normalizePath(Args.getInstance().getStorage().getDbDirectory());
        String indexDirectory = normalizePath(Args.getInstance().getStorage().getIndexDirectory());
        Args.getInstance().getStorage().setDbDirectory(dbDirectory);
        Args.getInstance().getStorage().setIndexDirectory(indexDirectory);

        String rawOutputDirectory = Args.getInstance().outputDirectory;
        boolean outputDirectoryDefault = StringUtils.isBlank(rawOutputDirectory)
                || "output-directory".equals(rawOutputDirectory);
        if (outputDirectoryDefault && (isAbsolutePath(dbDirectory) || isAbsolutePath(indexDirectory))) {
            Args.getInstance().outputDirectory = "";
            log.info("Output directory cleared because storage directory is absolute.");
        }
    }

    private static String normalizePath(String path) {
        if (path == null) {
            return "";
        }
        return path.trim();
    }

    private static boolean isAbsolutePath(String path) {
        return StringUtils.isNotBlank(path) && Paths.get(path).isAbsolute();
    }

    private static String resolveStoragePath(String outputDirectory, String storageDirectory) {
        if (StringUtils.isBlank(storageDirectory)) {
            return outputDirectory;
        }
        if (isAbsolutePath(storageDirectory)) {
            return Paths.get(storageDirectory).toString();
        }
        return Paths.get(outputDirectory, storageDirectory).toString();
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

    static class BlockTask {
        long blockNum;
        BlockCapsule block;

        public BlockTask(long blockNum, BlockCapsule block) {
            this.blockNum = blockNum;
            this.block = block;
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

        @Parameter(names = {"--threads"}, description = "Number of threads for parallel processing")
        int threads = 16;

        @Parameter(names = "--help", help = true)
        boolean help;
    }
}
