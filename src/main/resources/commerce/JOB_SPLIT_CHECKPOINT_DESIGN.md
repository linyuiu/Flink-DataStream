# 交易作业职责、Checkpoint与资源规划

适用代码：`org.commerce`。本文件是新增的拆分说明，不替代原业务口径与恢复约束。

## 1. 作业职责

部署六个Job：CDC、公共事实、金额指标、精确去重、审计和维度目录。划分依据是吞吐、状态保留和故障影响范围；相同输入、口径及保留策略的指标共同计算。

默认`pipeline.mode=split`。`CommerceMetricsJob`是显式选择`integrated`时的集成入口，不与独立指标作业同时运行。每个逻辑作业固定UID、消费组、状态布局和结果表，恢复路径只属于对应的兼容拓扑。

## 2. 代码和链路

```text
MySQL 业务事务 + Outbox / 维度
                 │
         CommerceCdcJob
                 │
       Kafka ODS outbox ── CommerceFactJob ── Kafka DWD trade_fact.v2
                 │                              ├─ CommerceAmountMetricsJob ── metric_amount_partial
       Kafka 维度 Topic                          ├─ CommerceDistinctMetricsJob ─ metric_distinct_partial
                 │                              └─ CommerceAuditJob ────────── trade_event
       CommerceDimensionJob
                 │
             dim_catalog

metric_amount_partial + metric_distinct_partial
                 └─ metric_day_v2 / dashboard_v2（查询合并，不再新增 Flink 汇总热点）
```

`TradePipelines.facts` 与 `TradePipelines.metrics` 是生产入口和本地测试共用的拓扑构建方法。Source/Sink 在各 Job 边界创建，测试不复制一套业务计算代码。

| Job | 输出/职责 | 主要有状态部分 | 默认 CK 间隔 |
|---|---|---|---|
| CommerceCdcJob | MySQL Outbox/维度 → Kafka ODS | CDC snapshot split/binlog 进度、Kafka事务 | 10 秒 |
| CommerceFactJob | 校验、准入、eventId去重 → Kafka DWD | eventId内容指纹、Kafka offset/事务 | 10 秒 |
| CommerceAmountMetricsJob | 9项金额与可加计数 → 金额分片表 | 日维度分片的9个long、刷新/关闭timer、序列 | 10 秒 |
| CommerceDistinctMetricsJob | 支付人数、退款订单数 → 去重计数分片表 | 精确去重标记；日维度分片的2个long、timer、序列 | 30 秒 |
| CommerceAuditJob | DWD事实 → Doris审计明细 | 消费位点、sink事务/缓冲；无用户业务keyed state | 60 秒 |
| CommerceDimensionJob | 维度最新版本 → Doris | 维度ID对应的最新内容和版本 | 10 秒 |

金额组：下单数、取消数、支付数、退款笔数、支付件数、下单商品金额、支付GMV、退款额、原支付日净GMV。

去重组：支付人数、成功退款订单数。订单/用户跨维度成员不能直接相加去重；ALL维度单独计算。

## 3. 为什么这样隔离

1. **公共事实去重只保留一份。** 下游读取事务提交后的DWD事实，不复制eventId指纹；唯一事件由源库约束和事实Job共同保证。
2. **人数/退款订单去重可以单独扩容和调 CK。** 它们的状态取决于“日期×维度成员×实体”的去重基数，不等于订单数，也不等于只有两个数字。
3. **金额不等待去重 Job 的 checkpoint。** 金额路径没有精确用户集合，不受另一个 Job 的 checkpoint barrier/反压直接牵连。
4. **审计写入不阻塞核心计算。** 审计Doris导入变慢时，审计consumer自己积压，不阻止公共DWD发布、金额和人数Job提交CK。
5. **跨 Job 通过 Kafka 缓冲。** 独立 Job 并不等于绝对硬件隔离；共享Kafka、Doris、TaskManager资源仍会竞争。强隔离需结合独立应用集群、资源配额与平台调度。

代价：增加Kafka读写、更多Job运维和checkpoint元数据；查询需要合并两张表；各指标可见进度不再完全同步。拆分不是无成本优化，也不保证总磁盘或CPU一定下降。

## 4. 两张 Doris 表不能省略

金额 Job 和去重 Job **不向同一个 Unique Key 行分别写部分字段**。否则普通全行覆盖可能把对方指标覆盖为缺省值，两个作业的 update_seq 也不具有可比性。

新建结构在 `sql/doris-split.sql`：

- `metric_amount_partial`：只含金额组9个指标，独立 update_seq。
- `metric_distinct_partial`：只含去重组2个指标，独立 update_seq。
- `metric_day_v2`：对各表最新主键行做 UNION ALL，再按日期/维度 SUM；缺少的另一组指标补0。
- `dashboard_v2`：补维度显示名称和金额单位转换/客单价。

视图包含 `amount_updated_at_ms`、`distinct_updated_at_ms`，用于查看两组最近的输出时刻。它们不是全局一致水位，更不能单独证明所有分片追平。尚未追平时去重计数可能暂时为0或落后，报表应同时显示数据延迟；拆分Job不提供跨Job原子快照查询。

这些SQL已随代码提供，**没有连接真实Doris执行DDL或验证引擎查询**。本地测试按相同的“各表按主键/序列覆盖，再合并求和”语义核验结果。

## 5. 状态保留与 CK 大小

| 状态 | 当前配置/规则 | 为什么 |
|---|---|---|
| eventId指纹 | 处理时间TTL=online.event.days+2，默认9天 | 配合事件日绝对准入门，指纹清理后旧消息也不能重新入账 |
| 支付用户/退款订单精确标记 | 同为9天TTL | 一天一维度一实体，只保存标记，不在一个全站key放巨大HashSet |
| 金额分片累计 | online.metric.days=93；业务日+94天零点关闭 | 允许近期退款回扣原支付日净GMV |
| 去重计数分片累计 | online.distinct.metric.days=9；业务日+10天零点关闭 | 日UV/退款订单数不回改原支付日，不需要保留93天 |
| 维度最新状态 | 当前无TTL | 低频更新的商品/店铺也必须保留最新版本；生产需治理主数据基数 |

金额组单个数值数组由11个long变为9个，去重计数组只存2个。这里只是数值负载大小，**不能用72字节或16字节乘key数当作完整状态大小**：还包括key、TTL时间戳、序列、timer、RocksDB索引/SST、序列化与元数据。

事实及金额/去重Job均在事件日及后7个日历日内接纳数据。DWD入口先整条检查准入，再展开维度；超期进入fact-horizon修复流。金额的历史目标日另有93天保留范围。独立Job消费时间不同，仍可能在日期关闭边界出现一方已入账、另一方需补算；必须按目标Job及截止点对账，不能声称跨Job原子一致。

### 增量 checkpoint 的三个不同量

1. **Full / referenced state size**：该checkpoint引用的状态总量，恢复需要这些文件。
2. **本次 checkpointed bytes**：本次新写的状态/元数据，复用旧SST时通常小于总引用量。
3. **存储实际占用**：多个保留checkpoint共享文件后的去重占用，还受清理、compaction和上传临时文件影响。

开启RocksDB增量checkpoint不代表状态不增长，也不代表没有新消息时本次写入必为0。不能把所有checkpoint的总引用大小直接相加当对象存储账单，也不能仅看一次增量大小估计恢复耗时。

当前保持对齐checkpoint、最大并发checkpoint=1。出现反压时先定位慢算子/sink与资源瓶颈；不要只为降低对齐等待就盲目开启非对齐checkpoint，后者会把在途通道数据也纳入快照，可能增加CK与恢复I/O。评估时应同时看对齐耗时、checkpointed/full大小和通道状态，而不是只看“CK成功”。

## 6. 日百万订单的量级推算

容量假设：每天创建100万商家子订单，支付率20%，年退款前商品GMV20亿元，推导平均支付商品客单价约27.40元。真实支付率、单价和订单明细分布由业务报表校准。

按当前PROFILE：创建100万+取消64万+成功支付20万+成功退款5万≈189万事实/天，平均21.875事实/秒；50倍候选峰值约1093.75事实/秒。原始业务表行变更和Outbox事件不是同一数量。

- 新增DWD一份事实流。按3KB/条保守假设约5.67GB/天，14天约79.38GB逻辑量，3副本约238.14GB，尚未考虑压缩、索引与预留。实际DWD去掉CDC envelope后可能更小，应测量实际字节数。
- 金额、去重、审计三个consumer各自读取DWD；忽略协议等开销，消费者合计逻辑读量约17.01GB/天。不是只写一份Kafka就只读一遍。
- 金额与去重Job分别进行维度展开；当前实现共享展开函数但独立执行，因此会重复一部分解析/展开CPU。以20条增量/事件的粗预算，两个计算Job约两份展开工作，必须按真实订单行数和热点压测。
- 指纹状态约189万×9天=1701万个key的量级。实际在线业务日窗口、重复与输入分布会影响数量。
- 精确UV状态上界可按“日支付用户×触达维度成员×9天”估计，例如20万×12×9≈2160万key；退款订单标记另计。真实去重率与SKU/店铺基数不能用样例目录代替。
- 金额累计主要取决于活跃日期×维度成员×实际触达分片；去重计数累计改为约10个日历日，而非金额的94个日历日。整体节省量必须实测，不能仅按9/93线性推断。

### 每个 Job 的 slot 与资源起点

slot是并行任务的调度槽，不提供CPU硬隔离。默认slot sharing下，同一Job通常按最大算子并行度预算槽位。表中内存是工作负载预算，包含task heap、managed memory及网络预算；RocksDB在managed memory管理下的cache/write buffer不重复加算。配置TaskManager process memory时另算框架、metaspace、JVM overhead和未纳管native分配，并以平台内存模型核对。

| Job | 候选并行度/运行slot | 每slot CPU起点 | 每slot有效任务内存起点 | CK总引用状态粗估 | 主要容量驱动 |
|---|---:|---:|---:|---:|---|
| CommerceCdcJob | 4 | 1 vCPU | 2–4 GiB | 稳态通常 <100 MiB | snapshot split、schema、binlog位点、Kafka事务元数据；不把整张MySQL表复制进CK |
| CommerceFactJob | 4 | 1 vCPU | 4–8 GiB | 2–6 GiB | 约1701万个活动eventId指纹及RocksDB开销 |
| CommerceAmountMetricsJob | 8 | 1–2 vCPU | 8–12 GiB | 10–60 GiB，可能更高 | 93天金额分片key、序列和日期关闭timer；高SKU/SPU基数最敏感 |
| CommerceDistinctMetricsJob | 8 | 1–2 vCPU | 8–12 GiB | 4–15 GiB | 9天支付用户/退款订单精确标记，以及10天分片计数状态 |
| CommerceAuditJob | 2 | 1 vCPU | 2–4 GiB | 通常 <256 MiB | Kafka位点、Doris 2PC committable/事务元数据；无业务keyed state |
| CommerceDimensionJob | 2 | 1 vCPU | 4–8 GiB | 1–10 GiB | SKU、SPU、品类、品牌、店铺、地区最新JSON；假设50万–200万总维度行 |

这组配置的正常调度预算是28个slot，CPU/内存尚须单独验证。备用容量公式为`28 + 最大故障TM的slot数 + 同时发布所需额外slot数`：若最大TM为4槽且发布另需4槽，可从36槽评估；若最大TM为8槽且发布需要8槽，则至少44槽。36–40并非通用要求。独立Application Cluster分别为每个Job核算故障和发布资源。

CPU和内存不能只按平均21.875事实/秒配置。尖峰约1093.75事实/秒，金额和去重Job还会将每个事实展开为多个维度增量；RocksDB compaction、CK上传和Doris反压也会短时占用CPU、磁盘和native memory。建议金额/去重Job使用本地NVMe并限制同一TaskManager上的高状态slot数量，不要在一台机器上放满8个8–12 GiB状态slot后仍按32 GiB进程内存启动。

### CK 大小推算方法

上述CK区间是**总引用状态（full/referenced size）的容量起点**，不是每次都重新上传的字节数，也不是承诺值。第一次CK、普通savepoint或大规模compaction后，本次写入可能接近总引用量；稳态增量CK通常更小，但代码无法预先知道SST切分、压缩率、更新热点和对象存储复用情况，因此不写一个虚假的固定压缩比例。

- 事实Job：`189万事实/天 × 9天 = 1701万活动eventId`。容量文档按每key 150–300字节估算约2.55–5.10GB，再给元数据和波动留余量，得到2–6 GiB起点。
- 金额Job：状态key近似 `94个活跃日 × 每日实际活跃分片key`。本地样本约323字节/分片key；若每日30万–200万活跃分片，线性基数约9–61GB，故先按10–60 GiB准备。商品目录更大、每天触达更多分片或SST写放大时会超过该范围。
- 去重Job：精确标记近似 `9天 × 日期/维度/用户或退款订单唯一组合`，再加 `10天 × 活跃计数分片`。当前示例上界约2160万支付用户维度key，退款订单另计，因此先按4–15 GiB准备。
- 维度Job：近似 `全部维度行数 × 序列化JSON和RocksDB每key实际字节`。生产维度JSON可能远大于mock，必须用真实字段和SKU总量采样。
- CDC和审计Job：没有长期业务keyed state。若它们的CK持续达到GiB级，优先检查初始快照未完成、反压、Doris事务/committable积压以及是否产生大量channel state。

这些范围不含TaskManager本地RocksDB多个版本、compaction临时文件和对象存储保留多个CK后的物理占用。磁盘/对象存储初始容量可按“最大总引用状态×2–3倍临时与版本余量”评估，再以长稳测试改正；不能简单把所有历史CK的full size相加，也不能把单次增量大小当恢复数据量。

### 上线后怎样把估算改成实测

对每个Job分别记录 `lastCheckpointSize`、`lastCheckpointFullSize`（版本支持时）、checkpoint duration、alignment、RocksDB live SST、本地磁盘、managed memory、compaction pending、Kafka lag和恢复耗时。至少完成：平均负载2倍24小时、候选尖峰30–60分钟、一个TaskManager退出恢复、Doris暂停产生反压四组测试。

扩容优先判断瓶颈：CPU/busy高而状态每slot可控时增加并行度；RocksDB/CK过大但CPU低时，增加slot只能摊薄单slot状态，不能减少总状态；金额Job若长期逼近60 GiB，应优先评估把“93天净GMV回扣状态”和“短期普通金额指标”进一步拆开，而不是只增加TaskManager内存。

## 7. 本地已做的测试与边界

### 多指标链路测试

运行：

```bash
mvn -q -Dtest=CommerceSplitPipelineTest test
```

测试使用生产共享的 TradePipelines、本地Flink MiniCluster、RocksDB、真实keyBy/shuffle/处理时间timer；并行度1和4均执行，并行度4同时开启对象复用。

主要场景：6张订单、4次成功支付、同用户多次购买、同商品多条明细、支付失败无成功事实、取消、退款失败无成功事实、两笔部分退款、一笔全额商品退款、全量重复并乱序。人工预期值独立写出，没有调用同一个计算函数生成“正确答案”。

预期全站结果：下单6单、取消1单、支付4单、支付3人、支付5件、退款2单/3笔、下单金额210元、支付GMV150元、退款60元、净GMV90元。共核验18个日维度行的全部11个基础指标。

另测跨日退款、同一eventId内容冲突、非法JSON、Outbox更新、金额不守恒、未来事件、超事件期限和超金额历史期限，检查quality/repair未被漏接。

这里将DWD边界在内存中直接连接，以测试业务拓扑组合。**不证明真实Kafka事务、跨Job暂停/恢复、Doris2PC、网络隔离或生产吞吐已经验收。** 原有状态恢复测试继续保留。

### 入站契约和积压边界测试

```bash
mvn -q -Dtest=TradeInputContractTest,TradeAdmissionTest test
```

`TradeInputContractTest`验证整数金额/数量、必填字段、重复JSON字段、尾随内容、Outbox归档删除与非法更新、交易/维度Kafka key、生产Checkpoint及事务预算、并行度上限和异常来源标注。

`TradeAdmissionTest`验证准入关闭前后1毫秒的边界、未来时钟、审计保留历史事实，并在实际共享指标拓扑中分别验证金额与去重Job将20天积压事实送入repair，而不更新在线小计。

### 小样本 checkpoint 测试

运行：

```bash
mvn -q -Dtest=CheckpointFootprintTest test
```

使用真实EmbeddedRocksDB增量后端和官方算子测试工具，构造1000订单/2200事实（每10单一单两次部分退款），分别snapshot事实指纹、精确去重、金额累计和去重计数累计；完成第一次checkpoint后，无新输入再做第二次。

本机多次观测的约值/范围（不等于生产配置下的整Job CK）：

| 状态组件 | 首次总引用/本次写入 | 无新输入时总引用 | 无新输入时本次写入 |
|---|---:|---:|---:|
| 事实指纹 | 约150 KiB | 约150 KiB | 约36 KiB |
| 精确去重标记 | 约240–242 KiB | 约240–242 KiB | 约155 KiB |
| 金额累计 | 约1.06–1.09 MiB | 约1.06–1.09 MiB | 约0.78–0.81 MiB |
| 去重计数累计 | 约660–664 KiB | 约660–664 KiB | 约398–399 KiB |

该小样本的金额累计比精确去重标记更大，说明不能仅凭“UV状态通常重”断定任意数据集的排序。实际基数、timer、状态句柄和序列化开销都要看；9天历史UV及93天活跃金额日未在这一小样本中铺满。此次事实/精确去重timer均为0，金额与计数累计分别保留3438/3517个日期关闭timer；raw keyed bytes均为0，状态在managed keyed部分。测试会打印这些信息辅助定位，不把比例外推为生产结果。数值会随时间、SST压缩和运行环境变化。

真实线上需要同时收集：每Job full/checkpointed size、CK耗时与失败率、SST/compaction、key基数、Kafka lag、恢复时长和Doris可见延迟。当前没有执行日百万订单的长期集群压测。

## 8. 部署步骤

1. 确认唯一写入者、目标表、业务截止点及可恢复的Checkpoint，保存发布前的配置和状态信息。
2. 在隔离环境执行原 `sql/doris.sql` 的公共表结构，再执行 `sql/doris-split.sql`；生产调整副本/分桶与历史保留期。
3. `CreateTopics`创建trade_fact、split_quality、split_repair等所需Topic，名称由配置读取。已有Topic不会自动改配置。
4. 按依赖顺序启动CDC、事实、金额、去重、审计、维度Job。入口类均在 `org.commerce.job`，同一jar用不同Entry Class分别提交。
5. 在联调环境跑小批mock，等consumer和sink追平后核对`commerce.dashboard_v2`。报表数据源必须指向这套分拆结果视图。
6. 核验资源和监控后逐级增加mock速率。禁止直接向生产执行压测、清表或重置offset。

本地示例入口：

```text
org.commerce.job.CommerceCdcJob
org.commerce.job.CommerceFactJob
org.commerce.job.CommerceAmountMetricsJob
org.commerce.job.CommerceDistinctMetricsJob
org.commerce.job.CommerceAuditJob
org.commerce.job.CommerceDimensionJob
```

正常运行显式选择split或integrated模式；影子验证使用隔离的Topic、表、事务前缀和消费组，避免两个独立计算实例覆盖同一累计表。

## 9. 恢复、历史迁移和一致性限制

- 每个Job只恢复自己的兼容checkpoint，并保持其Kafka进度、状态与Doris事务一致；不能给金额Job使用事实Job的checkpoint。
- 不兼容的状态拓扑需要显式转换或完整历史bootstrap。当前没有通用状态转换器，RUNNING不能证明历史基线已完成。
- 有历史业务时，必须从完整权威归档重建基线并衔接增量，或开发并验证显式状态迁移。普通线上7日准入、Kafka14日保留不足以重建93日金额状态。
- 空金额状态先收到30天前支付订单的退款时，虽然目标日在93日范围内，缺少支付基线仍会得到负净额。存量启动必须先完成历史基线，已有Doris行不等于Flink已恢复对应状态。
- 旧于93日的原支付日修正仍进repair，当天退款额可以正常更新。不能为补历史净额把同一整条退款重新投递并重复扣减当天退款。
- DWD唯一写入者为公共事实Job，消费者使用read_committed。下游不重复维护eventId去重表的前提是这一契约成立；禁止随意手工向DWD发送重复事实或无状态重放同一个事实Job到原Topic。
- 拆分只隔离checkpoint协调，不构成跨Job事务。金额10秒CK、去重30秒CK、审计60秒CK，最终可见时间不同。

## 10. 延迟与上线验收

CDC与事实层各10秒CK，再加金额10秒/去重30秒/审计60秒CK及分片刷新、处理、网络、Doris发布与查询，金额端到端可能经历约30秒以上的等待，去重与审计更久。不能再把单体10秒CK或3秒flush当作整个新链路延迟。

先测分段延迟，再选择CK间隔；若业务要求金额秒级，可以在压测证明成本可接受后缩短公共事实/金额CK间隔，不能未经测量直接所有Job改1秒。跨Job读到不同进度的指标时，应展示延迟或约定统一报表截点。

上线验收包含Kafka/Doris事务联调、独立暂停/恢复各Job、故障注入、历史基线对账与高SKU基数长稳压测。资源表作为压测输入，SLA由目标集群的结果确定。
