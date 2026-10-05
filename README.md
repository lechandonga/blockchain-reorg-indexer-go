# blockchain-reorg-indexer-go

一套完全本地运行的链上数据索引服务：用本地构造的区块模拟链上数据源，把
**区块摄入 → 交易解析入账 → 地址/哈希查询** 串成一条可重复运行的流水线，
保证乱序、重复投递和链重组（reorg）下索引结果一致且可对账。无任何外部
节点或网络依赖。

## 快速开始

```bash
# 运行全部单元测试（乱序 / 重复 / 损坏 / 重组 / 重启恢复 / 幂等重跑）
go test ./... -v

# 运行端到端演示流水线（模拟数据源 + 五个阶段 + 重启续跑）
go run ./cmd/indexer -data-dir ./index-data
# 再跑一次同样的命令，可观察到从持久化进度恢复、已处理区块判重
```

演示日志会打印每个区块的处理结果（`applied` / `duplicate` / `pending` /
`corrupted` / `reorg_applied`）以及最终的地址余额与交易列表。

## 数据模型

```
Block { Height, Hash, PrevHash, Timestamp, Txs[] }
Tx    { Hash, From, To, Amount }
```

- 区块哈希 = `sha256(Height, PrevHash, Timestamp, Txs)`，摄入时重新计算并
  校验，哈希不符即视为内容损坏。
- 创世块高度为 0，`PrevHash = "GENESIS"`。
- 索引内部维护（`internal/indexer/indexer.go`）：
  - `chain`：当前规范链（canonical chain）的区块序列；
  - `txs`：交易哈希 → `TxRef{Tx, BlockHash, BlockHeight}`；
  - `byAddr`：地址 → 规范链上的交易哈希列表（按链上顺序，自转账只记一次）；
  - `balance`：地址 → 已确认余额（收入为正、支出为负，仅用于对账演示）；
  - `pending`：父块尚未索引的暂存区块。

## 摄入规则与处理结果

每个区块的摄入返回一个可区分的状态（`internal/indexer/indexer.go` 的
`Status`）：

| 状态 | 含义 | 对索引的影响 |
|---|---|---|
| `applied` | 追加到规范链顶端（或父块补齐后从暂存区补入账） | 入账 |
| `duplicate` | 已入账或已暂存 | 无 |
| `pending` | 父块缺失（乱序/分叉未知），进入暂存区，父块到账后自动补入账 | 无 |
| `corrupted` | 哈希不符、字段缺失、金额非正、块内交易重复 | 无 |
| `reorg_applied` | 触发重组，旧分支回滚后该块入账 | 回滚 + 入账 |
| `reorg_too_deep` | 分叉点超出 `MaxReorgDepth`，拒绝 | 无（索引保持可查询） |
| `height_mismatch` | 高度与父块不连续 | 无 |

任何坏输入都不会中断流水线，也不会写坏已有索引；乱序区块在依赖补齐后
自动继续处理。

## 重组与回滚规则

- 新块的父块在规范链上但不是链顶时触发重组：先**逐块回滚**链顶到分叉点
  （撤销交易索引、地址索引与余额），再把新分支块依次补齐。
- 回滚 + 补齐在**同一把写锁**内完成：并发查询只会看到重组前或重组后的
  完整状态，永远读不到中间态（有专门的并发测试覆盖）。
- 回滚深度超过 `MaxReorgDepth` 时整体拒绝，索引保持原样、可继续查询，
  不存在"回滚一半"的状态。
- 被回滚分支上的区块若再次被投递，因其父块已不在规范链上，会进入
  `pending` 而不是污染索引。

## 持久化与重启恢复

- 每次成功变更后，将完整索引以**原子写**（临时文件 + rename）持久化到
  `DataDir/snapshot.json`。
- 重启时从快照恢复规范链与全部索引：已处理区块判重（`duplicate`）、
  新块从已持久化的高度继续，已确认数据不丢失。
- 同一批输入对全新目录或已持久化目录重复运行，最终索引状态完全一致
  （`TestIdempotentRerun` 验证）。

## 可配置项

代码配置（`indexer.Config`）：

| 字段 | 默认 | 说明 |
|---|---|---|
| `DataDir` | `""`（纯内存） | 快照持久化目录 |
| `MaxReorgDepth` | `64` | 可接受的最大重组回滚深度 |
| `Logger` | `nil` | 流水线事件日志输出 |

演示程序命令行参数（`cmd/indexer`）：`-data-dir`、`-max-reorg-depth`、
`-seed`（模拟数据源的随机种子）。

## 本地验证方法

```bash
go vet ./...
go test ./... -v        # 每个测试对应一类业务场景，日志可见通过情况
go run ./cmd/indexer -data-dir /tmp/idx && go run ./cmd/indexer -data-dir /tmp/idx
```

测试覆盖（`internal/indexer/indexer_test.go`）：

- `TestSequentialIngestAndQuery`：顺序摄入与按哈希/地址/余额查询；
- `TestDuplicateDeliveryNotReapplied`：重复投递不重复入账；
- `TestOutOfOrderDelivery`：乱序投递暂存、依赖补齐后自动入账；
- `TestCorruptedBlockRejected`：哈希损坏 / 非法交易被拒绝且索引无损；
- `TestReorgRollsBackAndAppliesNewBranch`：重组完整回滚旧分支、补齐新分支，
  无重复、无遗漏、无指向旧分支的残留；
- `TestReorgTooDeepRejected`：超深重组明确拒绝且索引保持完整可查；
- `TestRestartResumesWithoutReapply`：重启后从持久化进度续跑；
- `TestConcurrentQueryDuringReorg`：重组并发查询不读中间态；
- `TestIdempotentRerun`：同批输入重复运行最终状态一致。
