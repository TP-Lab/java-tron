# BlockTransactionPrinter 使用说明

## 简介

`BlockTransactionPrinter` 是一个只读数据库查询工具，用于从 TRON 本地节点数据库中导出指定区块范围或单笔交易的详细数据，支持多种输出格式和 Kafka 消息推送。

---

## 运行方式

### 区块范围模式

```
BlockTransactionPrinter <startBlockNum> <endBlockNum> [options]
```

### 单笔交易模式

```
BlockTransactionPrinter -tx <transactionId> [options]
```

---

## 参数说明

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `startBlockNum` | long | - | 起始区块号（>= 0） |
| `endBlockNum` | long | - | 结束区块号（>= startBlockNum） |
| `-tx <transactionId>` | string | - | 64 位十六进制交易 ID，启用单笔查询模式 |
| `-c <config_file>` | string | 默认配置 | 自定义配置文件路径 |
| `-d <data_dir>` | string | 默认数据目录 | 自定义数据库目录路径 |
| `-fm <format>` | string | `both` | 输出格式，见下方格式说明 |
| `-kb <brokers>` | string | 无 | Kafka broker 地址，如 `localhost:9092` |
| `-kt <topic>` | string | 无 | Kafka topic 名称 |
| `-kafka-rate <rate>` | int | `0`（无限制） | Kafka 发送速率限制（条/秒） |
| `-threads <count>` | int | CPU 核心数 × 2 | 并发处理线程池大小 |

---

## 输出格式（`-fm`）

| 格式值 | 说明 |
|--------|------|
| `json` | 以 JSON 格式输出 TransactionInfo 和 Transaction |
| `protobuf` | 以 Protobuf 文本格式输出 |
| `both` | 同时输出 JSON 和 Protobuf（默认） |
| `trigger` | 输出 `TransactionLogTrigger` JSON 结构（事件触发器格式） |
| `trigger-proto` | 输出 `TransactionLogTriggerPB` 序列化后的 Base64 字符串 |

> Kafka 推送仅支持 `trigger` 和 `trigger-proto` 格式。若指定了 Kafka 参数但格式不匹配，Kafka 输出将自动禁用。

---

## 使用示例

### 导出区块 1000 到 2000 的所有交易（默认 both 格式）

```bash
java -cp ... org.tron.program.BlockTransactionPrinter 1000 2000
```

### 导出为 trigger JSON 格式并推送到 Kafka

```bash
java -cp ... org.tron.program.BlockTransactionPrinter 1000 2000 \
  -fm trigger \
  -kb localhost:9092 \
  -kt tron-transactions \
  -kafka-rate 500
```

### 查询单笔交易（JSON 格式）

```bash
java -cp ... org.tron.program.BlockTransactionPrinter \
  -tx a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2 \
  -fm json
```

### 指定自定义配置和数据目录，使用 trigger-proto 格式

```bash
java -cp ... org.tron.program.BlockTransactionPrinter 5000 6000 \
  -c /path/to/config.conf \
  -d /path/to/database \
  -fm trigger-proto \
  -kb kafka1:9092,kafka2:9092 \
  -kt tron-proto-topic \
  -threads 16
```

---

## 注意事项

- 程序以**只读模式**打开数据库，不会修改任何数据。
- 区块号必须在数据库实际存储范围内，程序启动时会打印可用范围。
- `-tx` 模式下交易 ID 必须是 **64 位十六进制字符串**。
- `-kb` 和 `-kt` 必须同时指定，否则校验失败退出。
- 区块当前以每批 **200 个**为单位处理，多个区块的交易在线程池中并行执行。
- 所有资源（数据库上下文、Kafka 连接、线程池）在程序退出时自动释放。

---

## 性能诊断日志

从 2026-03 的性能排查开始，`BlockTransactionPrinter` 增加了批次级性能画像日志，用于区分瓶颈到底在：

- 区块读取
- `transactionRetStore` 预取
- `transactionHistoryStore` 降级预取
- 逐笔 `TransactionInfo` 回退查询
- `TriggerBuilder` 构建
- JSON / protobuf 序列化
- Kafka 发送或控制台输出

### 日志示例

```text
Batch [1627996-1628195] | fetched 200 blocks (190 non-empty, 1250 txs)
| fetch=1ms, txRetPrefetch=0ms, process=4083ms
| prefetchedRetBlocks=0, prefetchedTxInfo=0
| processDetail: index=1ms, wait=64774ms, txWallSum=129914ms, txCapsule=0ms,
txInfoFallback=129585ms/1250 hit=1250, txFallback=0ms/0 hit=0,
txRetFallback=1ms/190 hit=0, historyPrefetch=..., trigger=88ms,
serialize=179ms, emit=56ms, ...
```

### 关键字段

| 字段 | 含义 |
|------|------|
| `fetch` | 区块读取耗时 |
| `txRetPrefetch` | 批次级 `transactionRetStore` 预取耗时 |
| `prefetchedRetBlocks` | 批次中命中的 `TransactionRetCapsule` 数 |
| `prefetchedTxInfo` | 批次中预取到的 `TransactionInfo` 总数 |
| `index` | 构建 `txId -> TransactionInfo` 索引耗时 |
| `txRetFallback` | 区块级 `transactionRetStore` 回退读取次数和耗时 |
| `historyPrefetch` | 区块级 `transactionHistoryStore` 预取次数、耗时和命中条数 |
| `txInfoFallback` | 逐笔 `wallet.getTransactionInfoById()` 的次数和耗时 |
| `txFallback` | 逐笔 `wallet.getTransactionById()` 的次数和耗时 |
| `trigger` | `TriggerBuilder.createTransactionLogTrigger()` 耗时 |
| `serialize` | JSON / protobuf 序列化耗时 |
| `emit` | Kafka 发送或 stdout 输出耗时 |
| `prefetchHit / prefetchMiss` | 交易级预取命中 / miss 统计 |
| `slowestBlock` | 当前批次最慢区块及其耗时 |

### 快速判断方法

- `fetch` 大：瓶颈在区块读取。
- `txRetPrefetch` 大：瓶颈在 `transactionRetStore`。
- `historyPrefetch` 大且 `txInfoFallback` 很小：说明 `transactionRetStore` 不可用，但 `transactionHistoryStore` 的区块级降级生效。
- `txInfoFallback` 大：仍在逐笔查 `TransactionInfo`，这是最优先需要消除的路径。
- `trigger` / `serialize` / `emit` 大：瓶颈已经不在数据库读取，应转向 CPU 编码或 Kafka 输出。

### 本次真实排查结论

在 2026-03 的一次真实排查中，日志显示：

- `prefetchedRetBlocks=0`
- `prefetchedTxInfo=0`
- `txRetFallback=1ms/190 hit=0`
- `txInfoFallback=129585ms/1250 hit=1250`

这说明：

1. 目标区间内 `transactionRetStore` 没有提供可用的区块级 `TransactionInfo`
2. 主瓶颈不是区块读取，也不是 `TriggerBuilder`
3. 真正的主瓶颈是每笔交易都走了 `wallet.getTransactionInfoById()` 回退查询

因此，后续优化的核心逻辑是：

1. 继续保留 `transactionRetStore` 作为首选路径
2. 若按块未命中，则直接使用 `transactionHistoryStore` 做区块级预取
3. 尽可能避免退化为逐笔 `wallet.getTransactionInfoById()`

> 更完整的复盘见：`docs/IO_OPTIMIZATION_EXPLANATION.md`
