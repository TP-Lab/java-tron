# TransactionHistory 顺序扫描离线导出方案

## 1. 背景

当前 `BlockTransactionPrinter` 在目标区间缺少可用 `transactionRetStore` 时，会退化为：

```text
block -> transactionHistoryStore.get(txid) * N -> TriggerBuilder -> serialize -> emit
```

在历史库为 `LevelDB` 的前提下，这条路径的主瓶颈很稳定：

- `block` 读取几乎不是问题
- `trigger` / `serialize` / `emit` 不是主问题
- 绝大多数耗时都落在 `transactionHistoryStore` 的 `txid` 随机读

典型日志：

```text
Batch [1917260-1917459] | fetched 200 blocks (200 non-empty, 3050 txs)
| fetch=4ms, txRetPrefetch=0ms, process=5639ms
| processDetail: index=88262ms, wait=262ms, txWallSum=1856ms, ...
| historyPrefetch=88260ms/200 infos=3050, trigger=146ms, serialize=385ms, emit=1312ms, ...
```

这说明继续在线微调线程数、cacheSize、单批大小，已经不可能带来数量级提升。

---

## 2. 关键事实

### 2.1 `transactionHistoryStore` 不能按块顺序读取

[TransactionHistoryStore.java](/c:/workspace/TP-Lab/java-tron/chainbase/src/main/java/org/tron/core/store/TransactionHistoryStore.java)

- key 是 `txid`
- value 是 `TransactionInfoCapsule`

因此它的物理顺序是：

```text
txid 字典序
```

而不是：

```text
blockNum 顺序
```

`txid` 是交易 `rawData` 的 hash，与 `blockNum` 没有邻近性。

所以：

- 不能对 `transactionHistoryStore` 做“按块 seek + 顺序扫”
- 不能把当前按块导出的读取模式，简单变成“顺序读取 historyStore”

### 2.2 “按批收集 txid 再范围扫描”也没有意义

假设一批有 `n` 个随机 `txid`，把它们排序后取：

- `min(txid)`
- `max(txid)`

这两个点覆盖的 keyspace 期望跨度约为：

```text
(n - 1) / (n + 1)
```

当 `n = 3050` 时，跨度约为：

```text
3049 / 3051 ≈ 99.93%
```

也就是说，对一批 3050 笔交易做 `min(txid) -> max(txid)` 的范围扫描，几乎等于扫完整张 `transactionHistoryStore`。

所以“先收集 txid，再按 txid 范围扫”不是可行方向。

### 2.3 但 `TransactionInfo` 的 value 里自带 `blockNumber`

[TransactionInfoCapsule.java](/c:/workspace/TP-Lab/java-tron/chainbase/src/main/java/org/tron/core/capsule/TransactionInfoCapsule.java)

`TransactionInfo` 本身包含：

- `id`
- `blockNumber`
- `blockTimeStamp`
- receipt / log / internal tx 等导出所需字段

这意味着虽然 `transactionHistoryStore` 不能按块 seek，但可以：

1. 顺序扫描整张 `transactionHistoryStore`
2. 读取每条 value 中的 `blockNumber`
3. 仅保留落在目标区间内的记录
4. 再按块范围重组用于导出

这就是新方案的基础。

---

## 3. 新目标

针对“一次性全量或超大区间导出”的场景，目标不再是修复 `transactionRetStore`，而是：

**直接从 `transactionHistoryStore` 做一次顺序扫描，生成导出所需的区块级临时数据，然后按块顺序完成最终导出。**

目标链路：

```text
historyStore 顺序扫描 -> 目标区间分桶 -> block 顺序回放 -> TriggerBuilder / serialize / emit
```

而不是：

```text
block 顺序扫描 -> historyStore 按 txid 随机读取 -> TriggerBuilder / serialize / emit
```

---

## 4. 适用场景

推荐：

- 全量导出
- 极大区间导出
- 一次性离线导出
- 目标库是 `LevelDB`
- 需要保留现有复杂输出语义

不推荐：

- 很小的区间导出
- 临时抽样导出
- 可直接命中 `transactionRetStore` 的区间

原因很简单：

- 新方案会顺序扫描整张 `transactionHistoryStore`
- 小区间场景下，全库扫描成本不划算

---

## 5. 推荐工具形态

建议新增离线工具：

```text
org.tron.program.TransactionHistorySequentialExporter
```

设计目标：

- 不修改源数据库
- 不回填 `transactionRetStore`
- 复用现有 `TriggerBuilder` / `TransactionProcessor` / `KafkaSender`
- 将 I/O 形态从随机读改为顺序读 + 顺序临时文件读写

### 5.1 当前代码骨架

当前第一版代码骨架已经落在以下类中：

- [TransactionHistorySequentialExporter.java](/c:/workspace/TP-Lab/java-tron/framework/src/main/java/org/tron/program/TransactionHistorySequentialExporter.java)
- [ExportBucketManifest.java](/c:/workspace/TP-Lab/java-tron/framework/src/main/java/org/tron/program/exporter/sequential/ExportBucketManifest.java)
- [HistoryShardWriter.java](/c:/workspace/TP-Lab/java-tron/framework/src/main/java/org/tron/program/exporter/sequential/HistoryShardWriter.java)
- [HistoryShardLoader.java](/c:/workspace/TP-Lab/java-tron/framework/src/main/java/org/tron/program/exporter/sequential/HistoryShardLoader.java)

当前实现边界：

- 已支持 `prepare / export / all` 三阶段
- 已支持顺序扫描 `transactionHistoryStore` 并按块范围分桶
- 已支持按桶回放 `blockStore` 并复用现有 `TransactionProcessor`
- 暂未加入更细粒度 checkpoint、压缩、并发 shard 构建与断点恢复

当前已补充的运行日志：

- `Prepare progress` / `Prepare summary`
  - 关注 `scanned`、`matched`、`malformed`、`matchRate`
- `Prepare bucket hotspot`
  - 关注最热块桶的记录数和 shard 大小
- `Export bucket`
  - 关注 `txCoverage`、`infoCoverage`、`incompleteBlocks`、`missingTx`
- `Export summary`
  - 关注全局导出覆盖率、缺失交易、load/fetch/process 时间占比
- `Export weakest bucket`
  - 关注最差覆盖率的块桶，便于后续定向排查

---

## 6. 核心思路

### 6.1 总体分两阶段

#### 阶段 A：顺序扫描 `transactionHistoryStore`

1. 打开 `transactionHistoryStore` iterator
2. 从头顺序扫描整张库
3. 对每条 `TransactionInfoCapsule`：
   - 解析 `blockNumber`
   - 如果不在目标区间，直接跳过
   - 如果在目标区间，按区块范围分桶写入临时 shard 文件

临时记录格式建议：

```text
blockNum(8 bytes)
txId(32 bytes)
payloadSize(4 bytes)
transactionInfoBytes(payloadSize bytes)
```

写入方式建议：

- shard 按连续块区间分桶，例如每 `10,000` 或 `50,000` 块一个文件
- 只做顺序 append
- 可选 LZ4/ZSTD 压缩

#### 阶段 B：按桶回放 block 并导出

对每个块桶按顺序处理：

1. 将该 shard 文件完整读入内存
2. 构建：

```text
Map<txid, TransactionInfo>
```

3. 顺序扫描对应块范围的 `blockStore`
4. 对每个 block 的每笔交易：
   - 从内存 map 取 `TransactionInfo`
   - 调用现有 `TransactionProcessor` / `TriggerBuilder`
   - 输出到 Kafka 或 stdout
5. 当前桶处理完，释放内存，继续下一个桶

这条链路里，`transactionHistoryStore` 只在阶段 A 被顺序扫描一次。

### 6.2 为什么不做全局 txid 外排序

因为 `TransactionInfo` value 已经带了 `blockNumber`。

所以没必要再走这条更复杂的路线：

```text
block -> 收集 txid -> txid 外排序 -> historyStore merge join -> 再按块重排
```

相比之下，直接按 `blockNumber` 分桶更简单：

- 逻辑更直观
- 不需要额外的全局 txid 排序
- 更符合 KISS

---

## 7. 预期收益

### 7.1 收益来源

当前模式的复杂度近似是：

```text
targetTxCount 次随机 LevelDB get
```

新方案的 I/O 模式近似是：

```text
1 次 transactionHistoryStore 顺序全扫
+ 1 次目标 TransactionInfo 顺序落盘
+ 1 次 shard 顺序读回
+ 1 次 blockStore 顺序扫描
```

磁盘最擅长的是顺序吞吐，不是大规模随机读。

因此，在：

- 全量导出
- 数亿到数十亿交易导出
- NVMe 顺序吞吐远高于随机读

这些前提下，新方案有机会带来**数量级级别**的提升。

### 7.2 不承诺收益的场景

以下场景不应承诺 10x：

- 只导几万到几百万笔交易
- 目标区间只占全库很小一部分
- 临时文件所在磁盘很慢
- 内存过小导致桶尺寸过碎

---

## 8. 资源成本

### 8.1 临时磁盘

新方案的代价不是数据库写入，而是临时文件。

临时文件体积大致为：

```text
目标 TransactionInfo 序列化大小
+ txId 与块号元数据
+ 可选压缩后的额外索引/清单
```

粗略估算：

```text
每笔记录 ≈ 32B(txid) + 8B(blockNum) + 4B(len) + TransactionInfoBytes
```

因此若是超大规模全量导出，临时空间仍可能是 TB 级。

区别在于：

- 回填是把副本长期写回数据库
- 新方案的临时文件导出后可以删除

### 8.2 内存

内存主要用于单个桶的 `txid -> TransactionInfo` 映射。

因此应提供参数：

- `-bucket-blocks`
- 或 `-bucket-target-bytes`

用来控制单桶大小，使其落在机器可承受范围内。

---

## 9. 推荐实现方式

### 9.1 复用现有导出逻辑

保留：

- [TriggerBuilder.java](/c:/workspace/TP-Lab/java-tron/framework/src/main/java/org/tron/program/exporter/TriggerBuilder.java)
- [TransactionProcessor.java](/c:/workspace/TP-Lab/java-tron/framework/src/main/java/org/tron/program/exporter/TransactionProcessor.java)
- [KafkaSender.java](/c:/workspace/TP-Lab/java-tron/framework/src/main/java/org/tron/program/exporter/KafkaSender.java)

只重构取数责任：

- `TransactionProcessor` 不再负责查 `transactionHistoryStore`
- 新工具先准备好块范围内的 `txid -> TransactionInfo`
- `TransactionProcessor` 只负责格式转换和输出

### 9.2 新增组件建议

建议新增：

- `TransactionHistorySequentialExporter`
- `HistoryShardWriter`
- `HistoryShardLoader`
- `ExportBucketManifest`

职责划分：

- `TransactionHistorySequentialExporter`
  - 流程调度
  - 参数解析
  - 分阶段执行
- `HistoryShardWriter`
  - 顺序扫描 `transactionHistoryStore`
  - 将目标记录写入 shard
- `HistoryShardLoader`
  - 读取单桶 shard
  - 构建内存 `txid -> TransactionInfo`
- `ExportBucketManifest`
  - 记录每个桶的块范围、交易数、文件大小、完成状态

---

## 10. 建议参数

```text
TransactionHistorySequentialExporter <startBlockNum> <endBlockNum> [options]

选项：
  -c <config_file>
  -d <data_dir>
  -tmp <temp_dir>
  -phase <prepare|export|all>
  -fm <trigger|trigger-proto|json|protobuf|both>
  -kb <brokers>
  -kt <topic>
  -threads <count>
  -bucket-blocks <count>
  -strict
  -keep-temp
```

默认建议：

- `-phase all`
- `-bucket-blocks 10000`
- `-strict` 开启

`-strict` 含义：

- 某块缺少任意一笔 `TransactionInfo` 时，记为失败块
- 不静默降级为 `wallet.getTransactionInfoById()`

---

## 11. 失败恢复

建议按桶做 checkpoint：

- `prepare` 完成一个 shard，更新 manifest
- `export` 完成一个桶，更新 manifest

这样中断后可以：

- 不重扫已完成的 `transactionHistoryStore` 输出文件
- 不重导已完成的块桶

---

## 12. 与离线回填方案的关系

[transaction-ret-backfill-plan.md](/c:/workspace/TP-Lab/java-tron/docs/transaction-ret-backfill-plan.md)

两者解决的问题不同：

- 回填方案：修复数据库形态，适合重复导出/长期复用
- 本方案：一次性离线导出最快到结果，适合只想出数

如果目标只是导出一次，本方案优先级更高。

---

## 13. 最终建议

对于当前这套：

- `LevelDB`
- `transactionRetStore` 缺失
- 导出语义复杂
- 目标是一次性大规模导出

推荐路线是：

**不要继续围绕 `BlockTransactionPrinter` 的在线取数路径做微调，也不要先回填再导出。**

应直接实现：

**`TransactionHistorySequentialExporter`：先顺序扫描 `transactionHistoryStore`，按块范围分桶，再按块顺序复用现有导出逻辑完成输出。**

这是当前最有希望拿到数量级提升的方向。
