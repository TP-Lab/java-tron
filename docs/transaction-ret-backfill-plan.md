# TransactionRet 离线回填方案

## 1. 背景

当前 `BlockTransactionPrinter` 的在线导出链路已经完成了几轮优化：

- 修复 `transactionRetStore` 范围扫描读放大
- 复用长生命周期 iterator
- 增加批次级性能画像日志
- 将逐笔 `wallet.getTransactionInfoById()` 回退替换为区块级 `transactionHistoryStore` 预取
- 定向放大热点库缓存

但在历史数据实际存储为 `LevelDB` 且目标区间缺少可用 `transactionRetStore` 记录时，瓶颈仍然稳定落在 `transactionHistoryStore` 的逐笔随机读上。

典型日志如下：

```text
Batch [1788060-1788259] | fetched 200 blocks (132 non-empty, 822 txs)
| fetch=0ms, txRetPrefetch=0ms, process=879ms
| prefetchedRetBlocks=0, prefetchedTxInfo=0
| processDetail: index=13627ms, wait=79ms, txWallSum=345ms, txCapsule=0ms,
txInfoFallback=0ms/0 hit=0, txFallback=0ms/0 hit=0, txRetFallback=1ms/132 hit=0,
historyPrefetch=13626ms/132 infos=822, trigger=36ms, serialize=91ms, emit=215ms, ...
```

从这条日志可以直接得出：

1. `block` 顺序读取不是瓶颈。
2. `transactionRetStore` 在目标区间不可用。
3. 当前主耗时几乎全部在 `transactionHistoryStore` 的历史读取上。
4. 继续在线调大 cache 或增加线程，只会得到边际收益。

因此，下一步最有价值的方向不再是继续在线优化，而是把这批历史数据离线转换成按块聚合的读取形态。

---

## 2. 目标

### 2.1 主要目标

提供一个离线工具，将现有 `transactionHistoryStore` 中按 `txid` 存储的 `TransactionInfo`，回填为按 `blockNum` 聚合的 `TransactionRetCapsule`，写入 `transactionRetStore` 或兼容的侧边索引库。

### 2.2 预期结果

回填完成后，`BlockTransactionPrinter` 的主读取路径应恢复为：

```text
block -> TransactionRetStore.getTransactionInfoByBlockNum(blockNum) -> 内存索引 -> 输出
```

而不是当前的：

```text
block -> transactionHistoryStore.get(txid) * N
```

### 2.3 成功判据

对目标区间再次跑导出时，应满足：

- `prefetchedRetBlocks` 接近非空块数
- `prefetchedTxInfo` 接近批次交易总数
- `historyPrefetch` 接近 0
- `txInfoFallback` 接近 0
- 总吞吐明显高于当前 `LevelDB + txid 随机读` 方案

---

## 3. 非目标

本方案不解决以下问题：

- 不改变 `TransactionInfo` 的 protobuf 结构
- 不重写历史数据的业务语义
- 不尝试在线热更新正在运行的全节点数据库
- 不依赖继续挤压 `LevelDB` 随机读性能

---

## 4. 当前代码事实

### 4.1 `transactionRetStore` 的读写模型

[TransactionRetStore.java](/c:/workspace/TP-Lab/java-tron/chainbase/src/main/java/org/tron/core/store/TransactionRetStore.java)

- key: `ByteArray.fromLong(blockNum)`
- value: `TransactionRetCapsule`
- `getTransactionInfoByBlockNum(blockNum)` 直接按区块号读取整块 `TransactionInfo`
- `getTransactionInfo(txid)` 会先查交易所在区块，再在该块的 `TransactionRetCapsule` 内遍历定位

### 4.2 `transactionHistoryStore` 的读写模型

[TransactionHistoryStore.java](/c:/workspace/TP-Lab/java-tron/chainbase/src/main/java/org/tron/core/store/TransactionHistoryStore.java)

- key: `txid`
- value: `TransactionInfoCapsule`
- 对 `LevelDB` 来说，这条路径天然是随机读

### 4.3 `TransactionRetCapsule` 的构造方式

[TransactionRetCapsule.java](/c:/workspace/TP-Lab/java-tron/chainbase/src/main/java/org/tron/core/capsule/TransactionRetCapsule.java)

- `new TransactionRetCapsule(blockCapsule)` 会初始化：
  - `blockNumber`
  - `blockTimeStamp`
- 后续通过 `addTransactionInfo(...)` 或 `addAllTransactionInfos(...)` 追加整块交易执行信息

### 4.4 运行时的优先级

[Manager.java](/c:/workspace/TP-Lab/java-tron/framework/src/main/java/org/tron/core/db/Manager.java)

当前 `getTransactionInfoByBlockNum(blockNum)` 的优先级是：

1. 先查 `transactionRetStore`
2. 未命中时，再取该块的交易列表
3. 对每笔交易去 `transactionHistoryStore` 查一次

这意味着：

- 只要 `transactionRetStore` 回填完成
- 现有查询接口、导出程序和上层业务都会自动受益
- 无需继续改运行时主逻辑

---

## 5. 方案对比

### 5.1 方案 A：直接回填 `transactionRetStore`

思路：

- 用历史区块和 `transactionHistoryStore` 重建 `TransactionRetCapsule`
- 直接写入现有 `transactionRetStore`

优点：

- 与现有运行时逻辑完全兼容
- `BlockTransactionPrinter`、`Manager`、`Wallet` 无需额外改读取顺序
- 所有依赖 `getTransactionInfoByBlockNum()` 的代码都会直接受益

缺点：

- 会修改现有数据库内容
- 需要更严格的停机、备份和校验流程

结论：

- 推荐作为主方案

### 5.2 方案 B：生成侧边索引库

思路：

- 新建一个 `blockNum -> TransactionRetCapsule` 的侧边 store
- 运行时改成：
  `transactionRetStore -> backfillStore -> transactionHistoryStore`

优点：

- 不直接改原始 `transactionRetStore`
- 风险更可控

缺点：

- 运行时需要增加一层读取逻辑
- 需要新增 store、配置和维护成本
- 最终效果与直接回填相比，没有额外收益

结论：

- 适合作为保守备选方案
- 不作为首选

### 5.3 方案 C：继续在线优化 `transactionHistoryStore`

思路：

- 继续调大 cache
- 继续调线程数
- 继续做微观批量读优化

结论：

- 不推荐继续投入主要精力
- 在 `LevelDB + txid 随机读` 模型下，收益已经接近上限

---

## 6. 推荐方案

推荐采用：

**离线回填现有 `transactionRetStore`**

整体原则：

1. 只在停机或数据库副本上执行
2. 默认只回填缺失的区块，不覆盖已有记录
3. 默认不写入部分成功的区块，避免生成不完整 `TransactionRetCapsule`
4. 提供 `scan`、`backfill`、`verify` 三种模式

---

## 7. 工具形态设计

建议新增离线工具：

```text
org.tron.program.TransactionRetBackfiller
```

建议参数：

```text
TransactionRetBackfiller [startBlockNum endBlockNum] [options]

选项：
  -c <config_file>
  -d <data_dir>
  -mode <scan|backfill|verify>
  -batch <count>
  -overwrite-existing
```

默认行为：

- 不传区块范围时，自动处理整个数据库范围
- 默认模式为 `backfill`
- 默认直接输出日志，不额外生成 report 文件

### 7.1 `scan` 模式

只扫描，不写入。输出：

- 总区块数
- 非空块数
- 已存在 `transactionRetStore` 的块数
- 可完整回填的块数
- 缺少 `TransactionInfo` 的块数
- 缺失交易条数
- 预估新增磁盘占用

### 7.2 `backfill` 模式

执行真实回填。默认行为：

- 跳过空块
- 跳过已存在 `transactionRetStore` 的块
- 仅当整块所有 `TransactionInfo` 都可恢复时才写入

### 7.3 `verify` 模式

对指定区间做一致性校验。验证内容：

- `transactionRetStore` 是否能读到对应块
- `TransactionInfo` 数量是否与区块交易数一致
- `txid` 集合是否一致

---

## 8. 数据流设计

### 8.1 主流程

对于区间 `[startBlock, endBlock]` 中的每个区块：

1. 顺序读取 `block`
2. 若区块无交易，跳过
3. 若 `transactionRetStore` 已存在该块，跳过
4. 遍历块内交易，拿到 `txid`
5. 用 `transactionHistoryStore` 读取每笔 `TransactionInfo`
6. 若有任意一笔缺失，则标记该块不可回填
7. 若全部齐全，则构造 `TransactionRetCapsule`
8. 以 `blockNum` 为 key 写入 `transactionRetStore`

### 8.2 伪代码

```java
for (BlockCapsule block : blocksInRange) {
    if (block.getTransactions().isEmpty()) {
        continue;
    }

    if (skipExisting && transactionRetStore.getTransactionInfoByBlockNum(blockNumKey) != null) {
        continue;
    }

    List<TransactionInfo> infos = new ArrayList<>();
    boolean complete = true;

    for (TransactionCapsule tx : block.getTransactions()) {
        TransactionInfoCapsule infoCapsule =
            transactionHistoryStore.get(tx.getTransactionId().getBytes());
        if (infoCapsule == null) {
            complete = false;
            break;
        }
        infos.add(infoCapsule.getInstance());
    }

    if (!complete) {
        log.warn("missing tx info for block {}", block.getNum());
        continue;
    }

    TransactionRetCapsule ret = new TransactionRetCapsule(block);
    ret.addAllTransactionInfos(infos);
    transactionRetStore.put(ByteArray.fromLong(block.getNum()), ret);
}
```

---

## 9. 并发与批次策略

### 9.1 推荐原则

回填本身仍然要读取 `transactionHistoryStore`，因此它的执行过程依然是 I/O 密集型。

建议不要一开始就追求很高并发，避免把 `LevelDB` 随机读压得更差。

### 9.2 初始建议

- 区块读取：顺序单线程
- 历史查询：低到中等并发
- 写入：单线程顺序写

建议初始参数：

- `batch = 200` 或 `500`

### 9.3 后续调优

若验证发现：

- 源盘是独立 NVMe
- 缺页不严重
- `LevelDB` 文件打开数充足

再逐步增加历史查询并发。

---

## 10. 一致性策略

### 10.1 默认策略：整块原子完整

只要区块内有任意一笔交易缺少 `TransactionInfo`，该区块默认不写入。

原因：

- `transactionRetStore` 的语义是“整块交易执行结果集合”
- 部分写入会让调用方误以为该块已完整回填
- 这会污染后续所有依赖 `getTransactionInfoByBlockNum()` 的逻辑

### 10.2 跳过已存在记录

默认行为等价于开启“跳过已存在记录”：

- 已有 `transactionRetStore` 的块不重复写
- 使回填过程具备幂等性
- 便于中断后续跑

### 10.3 进度恢复

建议每处理一个批次落一条进度日志，至少记录：

- 当前块号
- 已写块数
- 跳过块数
- 缺失块数
- 缺失交易条数

如后续需要断点续跑，再单独补 checkpoint 机制。

---

## 11. 磁盘与资源评估

### 11.1 磁盘占用

回填本质上是在已有 `transactionHistoryStore` 之外，再额外存一份按块聚合的 `TransactionInfo` 副本。

因此需要预留额外磁盘空间。

粗略估算方法：

1. 用 `scan` 模式抽样若干块
2. 统计单块 `TransactionRetCapsule.getData().length`
3. 乘以待回填非空块数

### 11.2 内存占用

该工具不需要将大区间全部放入内存，只需要：

- 当前批次的块列表
- 当前块的 `TransactionInfo` 列表
- 少量统计对象

因此内存压力可控。

### 11.3 执行时间

需要明确一点：

回填过程本身仍然依赖 `transactionHistoryStore` 的随机读，因此它本身不会很快。

但是这是一次性的离线成本。

它的价值在于：

- 把后续每一次在线导出
- 都从“按交易随机读历史”
- 变成“按区块顺序读聚合结果”

---

## 12. 实施步骤

### 12.1 第一步：实现 `scan`

优先实现只读扫描：

- 验证目标区间到底有多少块缺失 `transactionRetStore`
- 验证这批块中有多少可以从 `transactionHistoryStore` 完整恢复
- 先拿到真实覆盖率和空间估算

这是第一阶段必须完成的工作。

### 12.2 第二步：实现 `backfill`

在 `scan` 结果可接受后，再开放真实写入。

建议默认：

- 不传 `-overwrite-existing`
- `allow-partial = false`

### 12.3 第三步：实现 `verify`

对回填结果做抽样和全量校验：

- 交易数一致
- `txid` 一致
- 关键字段可反序列化

### 12.4 第四步：重新压测导出

再次执行 `BlockTransactionPrinter`，重点看：

- `prefetchedRetBlocks`
- `prefetchedTxInfo`
- `historyPrefetch`
- `process`
- `blocks/s`
- `tx/s`

---

## 13. 风险与规避

### 13.1 风险：在线写入与节点运行冲突

规避：

- 只在停机状态执行
- 或对数据库副本执行

### 13.2 风险：回填出不完整块

规避：

- 默认严格完整性校验
- 缺一笔就整块跳过

### 13.3 风险：磁盘空间不足

规避：

- 先做 `scan`
- 先做抽样空间估算
- 先备份 `transactionRetStore`

### 13.4 风险：配置导致写入无效

`TransactionRetStore.put(...)` 受 `transactionHistorySwitch` 控制。

规避：

- 工具启动时明确检查 `storage.transHistory.switch`
- 若未开启，则直接失败退出

---

## 14. 推荐落地顺序

推荐按下面顺序推进：

1. 先实现 `scan` 模式
2. 跑一段真实区间，确认可恢复率
3. 再实现 `backfill` 模式
4. 在数据库副本上试跑
5. 做 `verify`
6. 回到 `BlockTransactionPrinter` 再压测

---

## 15. 结论

在当前这批历史数据上，继续在线优化已经接近收益上限。

真正能把瓶颈从 `LevelDB + txid 随机读` 中拿出来的方案，是：

**用现有 `transactionHistoryStore` 离线回填 `transactionRetStore`，把历史数据重新组织成按块聚合的读取形态。**

推荐先实现：

- `scan`
- `backfill`
- `verify`

三段式工具链，而不是直接继续堆在线 cache 和线程数。

**文档更新时间**: 2026-03-20
