# TransactionHistorySequentialExporter 使用说明

## 简介

`TransactionHistorySequentialExporter` 是一个面向**大区间 / 全量历史导出**的只读离线工具。

它不依赖 `transactionRetStore`，而是：

1. 顺序扫描 `transactionHistoryStore`
2. 按块范围将目标 `TransactionInfo` 写入临时 shard 文件
3. 再按桶回放 `blockStore`
4. 复用现有 `TransactionProcessor` / `TriggerBuilder` 完成最终导出

适用场景：

- 目标区间缺少可用 `transactionRetStore`
- 历史库为 `LevelDB`
- 需要一次性导出大体量历史数据

---

## 运行方式

```text
TransactionHistorySequentialExporter <startBlockNum> <endBlockNum> [options]
TransactionHistorySequentialExporter <startBlockNum> -1 [options]
```

说明：

- `startBlockNum` 必填，且必须 `>= 0`
- `endBlockNum` 必填，且必须 `>= startBlockNum`
- `endBlockNum = -1` 表示自动取数据库最新块
- 例如 `1000000 -1` 表示从 `1000000` 一直导出到数据库最后一个区块

---

## 参数说明

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `startBlockNum` | long | - | 起始区块号 |
| `endBlockNum` | long | - | 结束区块号，支持 `-1` 表示最新块 |
| `-c <config_file>` | string | 默认配置 | 自定义配置文件路径 |
| `-d <data_dir>` | string | 默认数据目录 | 自定义数据库目录 |
| `-phase <phase>` | string | `all` | 执行阶段，见下方说明 |
| `-tmp <temp_dir>` | string | 数据目录下自动生成 | 临时工作目录 |
| `-bucket-blocks <count>` | long | `10000` | 每个 shard 覆盖的区块数 |
| `-fm <format>` | string | `trigger` | 输出格式，见下方说明 |
| `-kb <brokers>` | string | 无 | Kafka broker 地址 |
| `-kt <topic>` | string | 无 | Kafka topic 名称 |
| `-kafka-rate <rate>` | int | `0` | Kafka 速率限制，`0` 表示不限速 |
| `-threads <count>` | int | CPU 核心数 × 2 | 交易处理线程池大小 |
| `-keep-temp` | flag | false | 在 `export/all` 成功后保留临时 shard 文件；未全量导完时默认也会保留 |

---

## 阶段说明

| 阶段 | 说明 |
|------|------|
| `prepare` | 只顺序扫描 `transactionHistoryStore`，生成 bucket manifest 和 shard 文件 |
| `export` | 只读取现有 manifest / shard，按块回放并导出 |
| `all` | 先 `prepare`，再 `export` |

推荐使用方式：

- 首次跑大区间：`-phase all`
- 需要重复验证导出逻辑：先 `prepare`，后多次 `export`
- `export` 阶段要求 `-tmp` 指向已存在的 manifest 目录
- `export` 阶段会以 `-tmp` 中的 manifest 为基础，只处理与命令行区块范围相交且 `exported=false` 的 bucket
- `nextExportBlockNum` 是**桶内前缀 checkpoint**：同一个 `-tmp` 目录只支持从当前 checkpoint 向后续跑，不支持跳过未导出的桶内前缀直接导更靠后的子区间
- 如果 `-tmp` 是旧版本导出器留下的目录，并且已经写入过部分 checkpoint，当前版本会拒绝继续复用；请重新 `prepare` 或使用新的 `-tmp`

---

## 输出格式（`-fm`）

| 格式值 | 说明 |
|--------|------|
| `trigger` | 输出 `TransactionLogTrigger` JSON |
| `trigger-proto` | 输出 `TransactionLogTriggerPB` 的 Base64 字符串 |
| `json` | 输出传统 JSON 文本 |
| `protobuf` | 输出传统 Protobuf 文本 |
| `both` | 同时输出 JSON 和 Protobuf 文本 |

说明：

- Kafka 仅支持 `trigger` 和 `trigger-proto`
- 若指定 Kafka 参数但格式不是这两种，Kafka 会自动禁用
- 启用 Kafka 时，仍会保留异步失败回传与 `flush` 校验

---

## 临时目录结构

默认情况下，临时目录会创建在数据目录下：

```text
<data_dir>/sequential-export-<start>-<end>/
```

目录内容：

- `manifest.properties`
- `buckets.tsv`
- `checkpoints.log`
- `history-<bucketStart>-<bucketEnd>.bin`

含义：

- `manifest.properties`：导出范围与桶大小
- `buckets.tsv`：每个桶的静态状态快照，包括准备状态、导出状态、记录数、字节数，以及最近一次落盘的 `nextExportBlockNum`
- `checkpoints.log`：`export` 阶段的轻量级追加式 checkpoint 日志；运行中优先靠它推进 `nextExportBlockNum`
- `history-*.bin`：该桶对应的 `TransactionInfo` shard 数据

---

## 使用示例

以下示例假设：

- 已在仓库根目录执行过构建，生成 `build/libs/framework-1.0.0.jar` 和 `build/libs/FullNode.jar`
- 当前环境为 Linux / macOS，classpath 分隔符使用 `:`
- Windows 环境请将 classpath 分隔符改为 `;`

### 只做 prepare：从指定起点扫到数据库最新块

```bash
java -cp "build/libs/framework-1.0.0.jar:build/libs/FullNode.jar" \
  org.tron.program.TransactionHistorySequentialExporter 5489445 -1 \
  -c ./main_net_config.conf \
  -d ./output-directory \
  -phase prepare \
  -tmp /data1/export-tmp \
  -bucket-blocks 1000
```

适用场景：

- 先顺序扫描 `transactionHistoryStore`
- 把目标区间切成 shard，供后续多次 `export` 复用
- 适合先确认 `transactionHistoryStore` 覆盖范围，再决定是否直接导出

### 基于已有 shard 做 export

```bash
java -cp "build/libs/framework-1.0.0.jar:build/libs/FullNode.jar" \
  org.tron.program.TransactionHistorySequentialExporter 5489445 -1 \
  -c ./main_net_config.conf \
  -d ./output-directory \
  -phase export \
  -tmp /data1/export-tmp \
  -fm trigger
```

适用场景：

- `prepare` 已完成
- 需要重复验证导出逻辑、格式或回退到 `transactionRetStore` 的行为
- 不想重复全扫 `transactionHistoryStore`
- 需要基于同一个 `-tmp` 目录，从当前 checkpoint 继续向后导出
- 当前实现会按成功窗口追加 checkpoint；若中途失败，重跑会优先从 `checkpoints.log` 中恢复 `nextExportBlockNum`
- 如果你想先导某个更靠后的独立子区间，请使用新的 `-tmp` 目录重新 `prepare`

### 一次跑完整链路：prepare + export

```bash
java -cp "build/libs/framework-1.0.0.jar:build/libs/FullNode.jar" \
  org.tron.program.TransactionHistorySequentialExporter 5489445 -1 \
  -c ./main_net_config.conf \
  -d ./output-directory \
  -phase all \
  -tmp /data1/export-tmp \
  -bucket-blocks 1000 \
  -fm trigger
```

适用场景：

- 首次跑某个大区间
- 希望一次完成 shard 准备和最终导出

### 导出到 Kafka 并限制速率

```bash
java -cp "build/libs/framework-1.0.0.jar:build/libs/FullNode.jar" \
  org.tron.program.TransactionHistorySequentialExporter 5489445 -1 \
  -c ./main_net_config.conf \
  -d ./output-directory \
  -phase all \
  -tmp /data1/export-tmp \
  -bucket-blocks 1000 \
  -fm trigger \
  -kb kafka1:9092,kafka2:9092 \
  -kt tron-trigger-topic \
  -kafka-rate 20000 \
  -threads 16 \
  -keep-temp
```

适用场景：

- `export` 阶段需要进一步压榨 CPU 与 Kafka 吞吐
- 目标是减少“每笔交易一个 future”的调度开销
- Kafka broker、topic 分区和网络都足够支撑更大的批量发送

### 从 `transactionHistoryStore` 分界点之后直接导到最新块

```bash
java -cp "build/libs/framework-1.0.0.jar:build/libs/FullNode.jar" \
  org.tron.program.TransactionHistorySequentialExporter 11016445 -1 \
  -c ./main_net_config.conf \
  -d ./output-directory \
  -phase all \
  -tmp /data1/export-tmp-11016445 \
  -bucket-blocks 10000 \
  -fm trigger
```

适用场景：

- 已确认 `11016445` 之后 `transactionHistoryStore` 基本无覆盖
- 需要依赖 `transactionRetStore` 回退补齐导出数据
- 当前实现要求导出完整性，若 shard 和 `transactionRetStore` 都无法完整覆盖区块，会直接失败退出

---

## 注意事项

- 程序以**只读模式**打开数据库，不会修改源数据库。
- 该工具更适合**大区间 / 全量导出**，不适合很小区间的临时抽样。
- `prepare` 阶段会顺序扫描整张 `transactionHistoryStore`，耗时取决于全库大小，不只取决于目标区间大小。
- `export` 阶段会优先使用 prepare 生成的 shard；如果 shard 中缺少某个区块的 `TransactionInfo`，会先尝试从 `transactionRetStore` 读取整块结果，仍不完整时会记录 `incompleteBlocks / missingTx / worstIncompleteBlock` 并**立即失败退出**。
- `-bucket-blocks` 越大，单桶 shard 越少，但单桶内存占用越高。
- 若需要重复调试 `export` 阶段，建议显式指定 `-tmp` 并保留 shard。
- 未指定 `-keep-temp` 时，只有 `export/all` 成功且 manifest 中所有 bucket 都已导完，才会自动删除临时目录；`prepare` 阶段或部分范围导出成功后会保留临时目录以支持续跑。
- `export` 阶段不再为每个区块重写整份 `buckets.tsv`，而是先追加 `checkpoints.log`，在程序正常结束时再落一次完整快照。
- 所有资源会在程序退出时自动释放。

---

## 常见现象与排查

### 现象 1：`prepare` 只生成到某个 `history-*.bin`，后面没有更多 shard 文件

先看 `buckets.tsv`：

- 如果后续 bucket 是 `prepared=true`、`recordCount=0`、`serializedBytes=0`，说明 `prepare` 已经**正常扫完整张** `transactionHistoryStore`。
- 这种情况下，后续 bucket 是**空桶**，不会生成对应的 `history-*.bin` 文件，这是正常行为。
- `history-*.bin` 只会在 bucket 首次命中记录时创建；空桶只会写进 `buckets.tsv`，不会落地空文件。

这通常表示：

- 从该区块范围开始，`transactionHistoryStore` 中已经没有更多命中的 `TransactionInfo`。
- 它只能证明“后续数据不在 `transactionHistoryStore` 里”，**不能仅凭这一点证明**“后续数据一定都在 `transactionRetStore` 里”。

### 现象 2：怀疑 `transactionRetStore` 不连续，是否会让 `prepare` 停掉

不会。

- `prepare` 阶段只依赖 `transactionHistoryStore`。
- `transactionRetStore` 是否连续，不会影响 `prepare` 是否继续，也不会影响 shard 是否生成。
- `transactionRetStore` 的连续性会影响 `export` 阶段能否在 shard 缺失时补齐整块数据，但不会影响 `prepare` 本身。

### 现象 3：想确认后续区块是否主要依赖 `transactionRetStore`

建议使用 `org.tron.program.TransactionRetBackfiller` 做只读检查。

说明：

- `scan`：统计指定区间内已有多少非空块能从 `transactionRetStore` 直接命中。
- `verify`：校验指定区间内已存在的 `transactionRetStore` 记录是否与区块交易列表一致。
- 该工具用于**停机节点或数据库副本**，不要对运行中的主库做在线排查。

### 现象 4：`export` 吞吐只有几千到一万 tx/s，但 CPU、磁盘都不高

优先看新增日志：

- `Export bucket ... processDetail`

判断方法：

- `processDetail.emit` 高：优先怀疑 Kafka 发送侧
- CPU、磁盘都不高且 `emit` 不高：通常是块级并行度不够或 topic 分区不足

示例：检查分界点附近 `transactionRetStore` 覆盖情况

```bash
java -cp ... org.tron.program.TransactionRetBackfiller 11014000 11026000 \
  -c /path/to/config.conf \
  -d /path/to/database \
  -mode scan \
  -batch 500
```

示例：校验后续区间 `transactionRetStore` 一致性

```bash
java -cp ... org.tron.program.TransactionRetBackfiller 11016445 11026444 \
  -c /path/to/config.conf \
  -d /path/to/database \
  -mode verify \
  -batch 500
```

重点关注日志汇总里的这些字段：

- `existingRet`：已存在 `transactionRetStore` 的非空块数
- `missingRet`：缺少 `transactionRetStore` 的非空块数
- `backfillable`：可以从 `transactionHistoryStore` 完整重建的块数
- `incomplete`：缺少部分 `TransactionInfo`、无法完整重建的块数
- `verifyFail`：`transactionRetStore` 校验失败的块数

经验判断：

- `existingRet` 很高、`missingRet=0`：后续区间主要依赖 `transactionRetStore`。
- `missingRet` 很高：后续区间并不完整地保存在 `transactionRetStore`。
- `verifyFail>0`：`transactionRetStore` 中已有记录存在不一致，需要单独排查。

---

## 运行日志

### Prepare 阶段

#### `Prepare progress`

示例：

```text
Prepare progress | scanned=1000000 matched=152340 skipped=847650 malformed=10
| matchRate=15.23% scannedBytes=4.21GB matchedBytes=638.10MB
| rate=28571 rec/s elapsed=35s
```

关键字段：

| 字段 | 含义 |
|------|------|
| `scanned` | 已扫描的 `TransactionInfo` 记录数 |
| `matched` | 落在目标区间内并写入 shard 的记录数 |
| `skipped` | 不在目标区间内的记录数 |
| `malformed` | 无法解析的坏记录数 |
| `matchRate` | 目标区间记录占全库扫描记录的比例 |
| `scannedBytes` | 已扫描 value 字节数 |
| `matchedBytes` | 已写入 shard 的 value 字节数 |
| `rate` | 当前 prepare 记录扫描速率 |

#### `Prepare summary`

用于看 prepare 总体是否正常：

- 目标区间命中比例
- 生成的 shard 总体积
- 非空桶数量

#### `Prepare bucket hotspot`

用于识别最热块桶，辅助后续调 `-bucket-blocks`：

- 哪个桶命中的记录最多
- 哪个桶的 shard 最大

### Export 阶段

#### `Export bucket start`

表示某个桶开始回放：

- shard 记录数
- shard 体积
- 是否已经被标记为 exported

#### `Export bucket`

示例：

```text
Export bucket [1000000-1009999] | blocks=10000 txs=65231 shardRecords=65190
| shardBytes=1.42GB load=812ms fetch=37ms process=15420ms
| emptyBlocks=120 nonEmptyBlocks=9880 exportedBlocks=9880 incompleteBlocks=0
| retFallbackBlocks=3 retFallbackTxs=41
| txCoverage=100.00% infoCoverage=99.94% missingTx=0
| worstIncompleteBlock=-1(0 tx, missing=0)
```

关键字段：

| 字段 | 含义 |
|------|------|
| `blocks` | 当前桶实际取到的区块数 |
| `txs` | 当前桶区块总交易数 |
| `shardRecords` | shard 中读取到的 `TransactionInfo` 条数 |
| `shardBytes` | shard payload 总字节数 |
| `load` | shard 加载耗时 |
| `fetch` | `blockStore` 读取耗时 |
| `process` | 交易处理与输出耗时 |
| `emptyBlocks` | 空块数 |
| `nonEmptyBlocks` | 非空块数 |
| `exportedBlocks` | 成功导出的区块数 |
| `incompleteBlocks` | 成功完成的导出中该值应为 `0`；若遇到不完整区块，导出会直接失败 |
| `retFallbackBlocks` | 通过 `transactionRetStore` 回退补齐并成功导出的区块数 |
| `retFallbackTxs` | 这些回退区块对应的交易总数 |
| `txCoverage` | 实际导出交易数 / 区块总交易数 |
| `infoCoverage` | shard 命中交易数 / 区块总交易数 |
| `missingTx` | 成功完成的导出中该值应为 `0`；若存在缺失交易，导出会直接失败 |
| `worstIncompleteBlock` | 当前桶缺失最严重的区块 |

#### `Export summary`

用于看全局导出质量与阶段耗时：

- 全局 `txCoverage`
- 全局 `infoCoverage`
- `transactionRetStore` 回退命中情况
- 全局 `missingTx`，成功完成的导出中应为 `0`
- `load / fetch / process` 总体占比

#### 导出失败

如果某个区块既无法从 shard 完整恢复，也无法从 `transactionRetStore` 完整补齐，当前实现会立即失败退出，典型日志类似：

```text
TransactionHistorySequentialExporter failed: Incomplete export data for block 12345678:
missingTx=2 txCount=157. Neither prepared shard nor transactionRetStore can fully cover this block.
```

这表示：

- 当前导出结果若继续执行将不再完整
- 需要先补齐 `transactionHistoryStore` 或 `transactionRetStore`
- 或先用 `TransactionRetBackfiller` / `verify` 检查缺口范围

---

## 参数调优建议

### `-bucket-blocks`

建议起点：

- 稀疏早期区间：`20000` 或更大
- 中后期高活跃区间：`5000 ~ 10000`

判断方法：

- 如果 `Prepare bucket hotspot` 的 `shardBytes` 很大，说明桶太大
- 如果桶数太多、`load` 时间占比高，可以适度放大

### `-threads`

建议起点：

- 先用默认值
- 如果 `Export bucket` 中 `process` 明显大于 `load + fetch`，再逐步提高

### `-tmp`

建议：

- 放在顺序写性能好的本地 NVMe 上
- 不要和高竞争的数据库目录混在慢盘上

---

## 与其他工具的关系

- [BlockTransactionPrinter.java](./BlockTransactionPrinter.java)
  - 适合已有 `transactionRetStore` 或中小区间在线导出
- [TransactionRetBackfiller.java](./TransactionRetBackfiller.java)
  - 适合修复数据库形态，供后续长期复用
- `TransactionHistorySequentialExporter`
  - 适合一次性大规模离线导出，目标是把 `txid` 随机读改成顺序扫描

> 设计背景与取舍说明见：
> [transaction-history-sequential-export-plan.md](../../../../../../docs/transaction-history-sequential-export-plan.md)
