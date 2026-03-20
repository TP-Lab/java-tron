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
- 区块以每批 **1000 个**为单位并发处理，多个区块的交易在线程池中并行执行。
- 所有资源（数据库上下文、Kafka 连接、线程池）在程序退出时自动释放。
