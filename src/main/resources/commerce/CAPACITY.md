# 容量模型与性能验收

## 1. 先区分设计输入与已测结果

当前按日创建 1,000,000 商家子订单、年退款前商品支付 GMV 20 亿、支付率 20% 建模。支付率/退款分布/峰值倍数都是可修改假设；代码可以处理的事件与状态规则不以这些比例为硬限制。

默认 PROFILE 分布：未支付订单中 80% 取消、20% 支付尝试失败；已支付订单中 5% 单次部分退款、5% 两次部分退款、5% 两次退款使商品金额全退、3% 退款失败、10% 支付失败后重试成功，其余正常支付。失败尝试不增加成功支付 Outbox，失败退款不增加退款成功 Outbox。

| 项目 | 计算/候选值 |
|---|---|
| 平均创建速率 | 1,000,000/86,400 = 11.57 单/秒 |
| 候选尖峰 | 平均×50 = 578.70 单/秒，不是观测到的真实峰值 |
| 成功支付订单 | 200,000/天；所需商品支付客单价约 27.40 元 |
| 成功退款事件 | 已支付订单×(5%×1+5%×2+5%×2)=50,000/天 |
| Outbox | 创建100万+取消64万+支付20万+退款5万=189万事件/天 |
| 峰值 Outbox | 1,890,000/86,400×50 = 1,093.75 事件/秒 |
| 指标扩展预算 | 暂按20条增量/事件≈21,875条/秒；订单行数、维度重复率会改变该值 |
| 业务库行变更粗估 | 假设16次行变更/单≈1,600万变更/天；须由真实 binlog 实测，未全量送入本指标链路 |
| Kafka 原始事件体积 | 假设3KB/事件≈5.67GB/天，14天≈79.38GB逻辑数据，3副本≈238.14GB，未含索引/事务/预留 |

峰值与事件展开倍数要用业务促销曲线、订单明细 P50/P95/P99 和热门 SKU 分布校准。200 行大单的展开量会远大于两行演示订单；复杂退款和支付率100%的模式也要单独压测。不能从平均每秒11.57单推断“单并行度一定够用”。

执行容量计算器（不访问外部服务）：

```bash
java -cp target/flink-datastream-job-all.jar org.commerce.ops.CapacityCalculator
```

## 2. 一组待压测的生产资源起点

这不是采购清单或压测结论。已有共享集群应按可用资源、查询压力和高可用要求合并评估。

| 组件 | 候选起点 | 必须观察 |
|---|---|---|
| MySQL | 主库+高可用副本；每实例8–16 vCPU、32–64GB内存、SSD/NVMe | 业务事务P99、锁等待、binlog增长、CDC快照对前台负载影响、磁盘容量 |
| Kafka | 3 brokers；每台8 vCPU、32GB内存、约1TB SSD/NVMe；交易Topic32分区、3副本、minISR2 | 分区倾斜、磁盘保留量、read_committed可见延迟、ISR、事务超时 |
| Flink | 按Job划分Application Cluster或资源池，备用槽按最大TM故障及发布并发计算 | 正常调度预算28 slot：CDC4+事实4+金额8+精确去重8+审计2+维度2；CPU和内存另行核算 |
| 状态存储 | 每TM本地NVMe留足RocksDB空间；checkpoint持久化到对象存储/HDFS | 实际增量checkpoint量、恢复带宽、compaction写放大、managed memory与磁盘余量 |
| Doris | 3 FE高可用；3 BE每台16 vCPU、64GB、NVMe；生产3副本 | Unique Key合并写入吞吐、compaction、事务提交/可见耗时、查询并发、tablet数量 |

配置候选在`production-candidate.properties`：CDC4、事实4、金额8、精确去重8、审计2、维度2、Doris sink8、Kafka32分区。split模式运行六个独立作业；integrated模式才使用集成指标入口。每Job的资源公式详见`JOB_SPLIT_CHECKPOINT_DESIGN.md`。Doris DDL的单副本配置用于开发，生产须核对副本、分桶和动态分区。

CDC snapshot 可以分片并行，但单 MySQL 实例增量 binlog 读取不能假设随并行度线性增长。如果存在多个业务分片，应规划独立 source/server-id 范围及稳定业务 ID 命名空间；当前代码只配置一个 MySQL 数据库。

## 3. 状态容量是关键，而不仅是吞吐

- eventId 指纹：189万/天×9天≈1,701万个活动key。假设每key总开销150–300字节，仅这一项约2.55–5.10GB逻辑量；RocksDB索引、SST、压缩、compaction放大以及checkpoint版本需实测。
- 精确支付用户：上界近似“日支付用户数×每事件涉及维度成员×9天”；例如20万用户/天×12个成员×9天≈2,160万key。真实同用户多订单会降低部分基数，但商品维度可能接近明细级基数。退款订单另计。
- 分片累计：`在线日期数 × 活跃维度成员 × 实际触达分片数`。93天不是每个key都创建32份，但生产百万SKU与演示1000SKU差异巨大。仅几十字节金额数组不代表整个key只有几十字节。
- eventId/精确去重使用RocksDB TTL和compaction回收，不为每个eventId注册清理timer；金额分片才使用刷新/日期关闭timer，且timer显式存RocksDB。
- 金额刷新按活跃分片合并写，每3秒一次，不是每条事件都向Doris发送全量指标。审计trade_event仍按已接纳事件逐条写，应计入单独的流量和存储。
- `trade_event`按3KB×189万×120天≈680.4GB逻辑原始事件，3副本≈2.04TB（未计压缩和其他开销）。可根据审计需求改为对象存储/湖仓长期保留，不能靠无限增加Doris动态分区解决。

分片规则属于持久状态与结果主键契约。调节parallelism不等于调节shards；修改shards不能直接写旧结果表，否则旧分片和新分片可能重复相加。

## 4. 延迟目标与一致性

CDC→Kafka和事实→Kafka各10秒Checkpoint；金额→Doris为10秒，精确去重为30秒，审计为60秒。分片刷新约3秒，另有排队、网络、Doris发布和查询耗时，必须逐段测量可见延迟。

验收目标按金额、人数和审计分别约定，并覆盖P95/P99及恢复阶段。先测各段事务提交和处理耗时，再按成本与时效要求调节Checkpoint周期。

`updated_at_ms` 是本分片计算刷新时刻；dashboard中的最大值不代表所有分片都追到最新事件。端到端延迟应以业务事件 occurredAt 与结果真实可查时间比较，并同时观测source进度。Flink sink的Records Sent=0表示没有下游Flink网络输出，不能用来衡量Doris写入量。

## 5. 已执行与尚未执行的验证

- `BusinessAndMetricsTest`：9类业务场景、支付/退款失败、450组乱序重复序列、跨支付日退款、订单/用户计数与分片守恒、契约错误。
- `StateRecoveryTest`：使用Flink官方KeyedOneInputStreamOperatorTestHarness执行snapshot/initializeState，验证eventId和distinct恢复去重、分片累计/序列恢复、TTL后旧数据隔离、历史日期关闭不从零覆盖。不是仅测试Java对象序列化。
- 纯CPU微基准：本地一次100,000事件测试，解析+维度扩展输出2,075,368条，约0.858秒/11.65万事件每秒。它不含Kafka、RocksDB、checkpoint、网络、MySQL或Doris，不能当作集群吞吐，也不能作为稳定性能承诺。
- **尚未执行**：真实数据库DDL验收、真实CDC快照/binlog切换、真实Kafka事务及Doris2PC故障验证、长稳状态容量/恢复时间/线上机器吞吐验收。

复现CPU测试：

```bash
java -cp target/flink-datastream-job-all.jar org.commerce.ops.EventMicroBenchmark 100000
```

## 6. 上线前验收矩阵

1. 功能：先PAID、再所有非OLD场景，逐订单对MySQL成功流水/退款分摊、Kafka成功事件、DorisALL与各维度金额；金额差异必须为0分。
2. 稳态：至少平均负载2倍运行24小时，记录真实平均事件大小、每事件展开量、checkpoint大小/时间、RocksDB和Doris增长。
3. 尖峰：约600创建单/秒持续30–60分钟，再逐级至1200单/秒或集群安全上限；检查可恢复积压和资源余量，不在生产直接压测。
4. 倾斜：80%金额集中单SKU/品牌/省份；一个用户重复下单；200行大单；多次退款。默认mock目录只有1000SKU，正式状态容量测试必须扩展目录基数到真实数量，不能只提高速率。
5. 故障：在checkpoint前后杀TaskManager、暂停sink、网络抖动、broker滚动、恢复兼容savepoint，确认无重复/丢失、未提交事务处理正确。故障注入必须在隔离环境执行。
6. 边界：跨日、延迟7日边缘、超过7日、退款原支付日93日边缘、超过93日、维度缺失与版本冲突、重复eventId内容变化。
7. 补算：完整归档重建到新命名空间；验证与线上分界点没有重叠或缺口，对账后再切换查询。

至少监控source lag、CDC snapshot状态/binlog可用起点、checkpoint成功率与年龄、任务重启、backpressure/busy、Kafka事务/ISR、Doris导入失败/可见时间、quality和repair流量与积压、对账金额差异。没有监控接入前不能宣称具备生产运维闭环。
