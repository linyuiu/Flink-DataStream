# Commerce：交易域实时指标独立模块

## 1. 定位与已实现范围

交易链路使用六个作业：CDC接入、公共事实、金额指标、精确去重、审计和维度目录。各自拥有消费进度、Checkpoint和输出职责。资源及状态设计见 [JOB_SPLIT_CHECKPOINT_DESIGN.md](JOB_SPLIT_CHECKPOINT_DESIGN.md)。

代码位于 `src/main/java/org/commerce`，在Maven工程中使用独立Java包和资源命名空间。运行栈为Flink 1.19.3、Flink CDC 3.2.1、MySQL、Kafka和Doris。

这是可编译、有业务与状态恢复测试的工程实现和容量候选方案；没有在真实 MySQL/Kafka/Doris 集群完成端到端验收，也没有证明生产吞吐 SLA。部署步骤见 [RUNBOOK.md](RUNBOOK.md)，容量与验证边界见 [CAPACITY.md](CAPACITY.md)。

业务范围是交易域：创建、取消、支付尝试、支付成功、商品金额退款和多维日指标。不扩展库存、物流、营销、结算等数据域；没有把“商品金额全退”等同于“运费也全退”“货物已退回”或“订单取消”。

### 规模假设必须先核对

- 日均 100 万暂解释为**创建的商家子订单**；年 20 亿元暂解释为退款前、扣优惠后、不含运费的商品支付 GMV。
- `20亿元 / 365 / 100万 = 5.48元/创建订单`。若支付率为 20%，支付订单平均商品金额约 27.40 元。20% 只是可调建模假设，不是行业通用支付率。
- 如果 100 万是已支付订单，隐含支付客单价就是 5.48 元；如果 20 亿元是净 GMV，则还需退款率推导支付规模。不能只按年 GMV 购买机器。

## 2. 为什么采用这条链路

```text
模拟器 / 真实业务服务
  ├─ MySQL 订单、明细、支付尝试、支付成功流水、退款申请与分摊
  └─ 同一数据库事务 INSERT trade_outbox（不可变业务事实）
                                │
                   CommerceCdcJob / Flink CDC
                                │
                   Kafka commerce.ods.trade_outbox
                                │
                    CommerceFactJob
        严格契约校验 → 时间准入 → eventId去重
                                │
                 Kafka commerce.dwd.trade_fact.v2
                  ├─ CommerceAmountMetricsJob → metric_amount_partial
                  ├─ CommerceDistinctMetricsJob → metric_distinct_partial
                  └─ CommerceAuditJob → trade_event
                                │
                   metric_day_v2 / dashboard_v2（SUM分片）

MySQL 六类维度 → 同一个 CDC Job → 六个 compact Kafka Topic
                                │
                    CommerceDimensionJob
                                │
                  Doris dim_catalog（版本覆盖）
                                └─ 查询时关联显示名称

交易质量问题 → commerce.quality.v2；超在线时限 → commerce.repair.v2
维度质量问题 → commerce.quality
已接纳业务事实 → Doris trade_event（审计，不替代长期原始归档）
```

两个模拟入口独立运行：`CommerceMySqlSimulator` 写 MySQL，`CommerceKafkaSimulator` 直接向配置的 `topic.trade_outbox` 及六个维度 ODS Topic 写消息。Kafka 入口无需启动 MySQL 或 CDC Job，从 `CommerceFactJob` 开始运行后续五个 Job 即可测试。两入口仅共用纯业务数据生成代码：订单抽样、`BusinessGenerator`、交易计划、Outbox 行构造和 `Catalog`，运行时互不调用。设置相同的 `mock.run.id`、种子、业务参数和 `mock.start.time.ms`，即使两个入口在不同时间单独运行，也会构造相同的业务表行、事件 ID、金额和业务时间；若不设置固定时间，各自按当前时钟生成，时间戳自然不同。使用方法见 [RUNBOOK.md](RUNBOOK.md)。Kafka 快速路径只发布当前下游实际消费的 Outbox 事实和维度记录，不在 MySQL 保存订单/支付/退款行，不能用于验证数据库事务、binlog 或 CDC 行为。

业务事务先完成权威金额分摊，再原子写业务表与 Outbox，避免数据侧把多个异步到达的表拼成“半条支付成功订单”。CDC 仍是读取 MySQL binlog，并不是应用直接发送 Kafka。

这是常用的 transactional outbox 模式，但**不是所有电商公司都必须使用的唯一方案**。它要求业务服务拥有事件定义并配合同事务写入；只靠数据团队不能把此要求假设为已经成立。真实订单、支付、退款分属不同数据库/服务时，不能跨库假装一个本地事务：应由各服务落自己的业务表与 Outbox，再由交易事实层协调；本模块限定为同一交易库内的权威事实。若只能拿到原始业务表 CDC，则需另建多表事实装配、版本与完整性判断、超时补偿链路，不能直接把原始订单表 CDC 塞进本 Job。

## 3. MySQL 表的业务含义与结构

完整可执行结构见 [sql/mysql.sql](sql/mysql.sql)。所有金额用 `BIGINT` 人民币分，时点用 epoch 毫秒，日历日期统一 Asia/Shanghai；IDs 为业务稳定标识，不使用 Kafka offset 作为业务主键。

| 表 | 粒度与主要字段 | 业务约束 |
|---|---|---|
| trade_order | 一张商家子订单；checkout_id、user_id、shop_id、收货省市区、status、goods_cent、freight_cent、payable_cent、version | 订单应付=优惠后商品额+运费；一个 checkout 可在真实服务拆多商家子单，本模拟器每次只创建一个子单 |
| trade_order_line | 子单中的一条购买明细；SKU/SPU/品类/品牌快照、quantity、unit_price_cent、discount_cent、payable_cent | 应付=单价×数量−优惠；尚未支付时不能叫已支付金额 |
| pay_attempt | 一次向支付渠道发起的尝试；order_id、amount_cent、status、failure_code | PENDING→FAILED 或 SUCCEEDED；失败必须可追查，重试使用新的 attempt_id |
| pay_ledger | 一笔已确认成功的支付；attempt_id、channel_txn_id、paid_at、amount_cent | 不保存失败付款；渠道流水与 attempt 唯一；当前限定每子单一次全额成功付款 |
| pay_allocation | 成功支付对一条订单明细的商品金额分摊 | 只含商品额，不含运费；同一子单的商品分摊总和=订单商品应付 |
| refund_request | 一次独立退款请求；payment_id、amount_cent、status、succeeded_at | APPLIED→PROCESSING→SUCCEEDED/FAILED；失败不减指标；多次退款不是反复覆盖一条“累计退款”消息 |
| refund_allocation | 退款请求对某条订单明细的退款金额 | 同请求同明细唯一；成功的累计退款不得超过该明细成功支付分摊 |
| trade_outbox | 一条不可变业务事实；id、order_id、business_id、event_type、occurred_at、payload_json | 主键 eventId；业务唯一键 `(event_type,order_id,business_id)`；与触发该事实的业务更新同事务提交 |

### 真实业务服务需要保证什么

1. 支付回调校验签名、商户号、币种、实收额；以渠道流水/请求幂等键和条件更新防重复落账，不能仅凭前端“支付成功”页计入。
2. 重复回调查到已成功的同一流水应返回幂等成功，而非生成新的 paymentId/eventId。模拟器的重复运行 ID 冲突会报错，用于防止误覆盖；它不是完整的支付回调 HTTP 服务。
3. 退款使用事务锁/条件更新管理“已退+退款处理中已占用额度”，失败释放占用，成功固化；防止两个并发退款分别校验通过后超退。模拟器按订单串行产生合法分摊，**没有实现真实多服务并发退款额度预占系统**。
4. 优惠在业务确认时按订单明细分摊，余分采用固定规则（例如最大余数法并以明细 ID 破平局），保留不可变结果；退款沿用原支付分摊，不能拿今天的商品价格计算历史退款。本模拟器直接构造守恒的两行金额，并非完整优惠引擎。
5. 订单、支付、售后分别建模：退款不把 pay_ledger 改成失败，也不直接把订单改成取消。交易模拟器不生成发货/完成状态。
6. Outbox 不更新、不手工改 eventId、不在未落账时先发成功事件。数据库权限、唯一约束、业务事务和日对账共同保证；Flink 校验无法证明一条格式正确但造假的退款确实获得渠道确认。

主从复制延迟、CDC 断流、Kafka lag、Doris 导入失败由平台与数据侧监控；支付/退款失败是正常业务结果，不能靠“让 Flink 抛异常”表达。异常数据进入质量流必须配告警、责任人和修复闭环。

## 4. 维度设计与保存地点

| 维度 | 业务权威来源 | MySQL / Kafka / Doris | 历史口径 |
|---|---|---|---|
| 商品 SPU、SKU | 商品中心 | dim_product、dim_sku → 各自 Topic → dim_catalog | 订单行保存购买时的 ID 与归属快照 |
| 品类、品牌 | 商品主数据管理 | dim_category、dim_brand → 各自 Topic → dim_catalog | 品类采用一个统计叶子层，不自动包含所有祖先品类 |
| 店铺 | 商家中心 | dim_shop → Topic → dim_catalog | 一子订单归属一个商家店铺 |
| 省、市、区 | 地址主数据；交易侧保存收货地区 | dim_region → Topic → 三种 dimension_type | 使用下单快照；不跟随用户后来修改的默认地址 |

本模块为了本地可运行，将六类主数据集中放在一个演示 MySQL 库；真实系统通常由不同业务中心维护。Flink 金额路径只使用交易事实携带的历史 ID，不做逐条 JDBC 查询，不将海量 SKU 全量广播到每个 TaskManager。Doris 查询时补最新名称，名称变化不会改历史金额所属品类。需要“历史名称”或按最新分类重述历史时，应另建 SCD2/历史映射与显式重算，当前未实现。

维度业务主键稳定、version 严格递增，停用使用软状态且保留可查询记录。当前物理删除进入 quality，不自动删除历史显示维度。Kafka 维度 Topic 仅 compact，保存每个 key 的最新版本；已有 Topic 的设置不会被创建工具偷偷改动。

样例目录为 100 店铺、500 SPU、1000 SKU、20 品类、10 品牌及两套省市区。商品 SKU 是平台标准目录，同 SKU 可被不同店铺销售，本模拟器省略店铺 offer/上架价模型，仅保留成交快照。**这些只是功能数据，不代表生产维度基数**。

## 5. 中间事件契约

Java 契约：`model/TradeEvent.java`；校验：`validation/EventContract.java`。

- 公共字段：schemaVersion=1、eventId、eventType、businessId、orderId、checkoutId、userId、shopId、provinceId/cityId/districtId、currency=CNY、occurredAt、paidAt、goodsCent、lines。
- 每行：orderLineId、productId、skuId、categoryId、brandId、amountCent、quantity。行金额相加必须等于事件 goodsCent，不允许重复 orderLineId，最多 200 行。
- ORDER_CREATED / ORDER_CANCELLED：goodsCent 是订单商品应付，paidAt=0；occurredAt 分别是创建/取消发生时刻。
- PAYMENT_SUCCEEDED：goodsCent 是商品成功支付额，paidAt=occurredAt；quantity 是购买数量。
- REFUND_SUCCEEDED：goodsCent 是**这一次**成功商品退款额，不是累计值；paidAt 保留原支付时刻；只包含退款涉及行，quantity=0，因为退款金额不代表退货件数。
- 同一 eventId 的相同内容视为重放；内容变化进入质量流。业务表唯一键还负责阻止“同一成功业务换了一个新 eventId”的重复。Flink 不以 Kafka key 判定业务身份。

MySQL JSON 列保存该完整事件；CDC 输出 Debezium envelope（source、op、after、before），`after.payload_json` 是事件 JSON 文本。解析层核对 envelope 中 id/order/type/businessId/time 与 payload 一致。outbox 首次快照 `r` 和新增 `c` 都接收，更新/删除不解释成新交易。

数值必须是JSON整数，标识必须是字符串。缺失schemaVersion、null时间、小数金额、数字字符串、重复JSON字段和尾随第二个JSON对象均拒绝，防止反序列化自动转换破坏金额口径。

Outbox删除是源库归档清理，不冲减已发布事实，记录`outbox_cleanup_deletes`；更新仍进入质量流。业务服务必须确认归档完成、CDC已读取后才能清理，CDC消费进度由平台监控。交易ODS和DWD的Kafka key为orderId，维度消息key为维度主键；Flink内部仍按eventId去重，不能用Kafka分区内顺序代替业务幂等。

## 6. 指标口径与链路

每个事实先在其订单内按维度归并，再发出增量。例如一个订单有两行同品牌商品：品牌 GMV 累加两行金额，但品牌支付订单数只加 1。一个订单包含两个品牌，则各品牌均有 1 单，**不能把各品牌订单数相加当全站订单数**。

| 指标 | 入账日 | 算法 |
|---|---|---|
| created_orders / created_goods_cent | 创建日 | 创建事实的订单数/商品应付 |
| cancelled_orders | 取消发生日 | 取消事实计数；不是创建日取消率分子 |
| paid_orders / paid_quantity / paid_goods_cent | 支付成功日 | 成功支付订单数、商品件数、退款前商品支付 GMV |
| paid_users | 支付成功日 | 日+维度+userId 精确去重 |
| refund_requests / refund_goods_cent | 退款成功日 | 每次成功退款请求数/商品退款额 |
| refund_orders | 退款成功日 | 日+维度+orderId 精确去重，多次退款仍只算当天一单 |
| net_paid_goods_cent | 原支付日 | 原支付 GMV−归属这些支付订单的后续成功商品退款 |

示例：9 月 1 日支付商品 100 元，9 月 3 日成功退款 20 元。9 月 1 日 paid_gmv=100、net_paid_gmv=80；9 月 3 日 refund_amount=20。9 月 3 日 `goods_cash_net=当天商品支付−当天商品退款` 可能为负，这不是原支付日净 GMV，也不是包含运费的财务净收入。

支付失败、退款失败没有相应成功事实，因此不会产生支付/退款金额增量。支付失败率、退款失败率不在已实现指标内：需扩展尝试事实及其专门口径，不能用当前成功流推导。

维度类型为 ALL、SHOP、PRODUCT、SKU、CATEGORY、BRAND、PROVINCE、CITY、DISTRICT。钱在同一维度类型的各成员间可守恒；订单、用户跨成员和跨日不可直接加总去重。支付客单价=商品支付 GMV/支付订单数。下单与支付的发生日可能不同，不能拿当天 paid_orders/created_orders 冒充同一批订单的支付转化率。

## 7. 热点、去重、恢复与迟到边界

- 全站按 32 分片、店铺等低基数维度按 8 分片、商品/SKU 按 4 分片。常规金额由 orderId 散列；支付用户首次增量按 userId 散列。每次支付用户只在一个分片计一次，SUM 分片可得精确日 UV。
- 最终写 Doris 的是每分片绝对累计值，Unique Key=`日期+维度类型+维度ID+分片ID`，`update_seq` 从持久状态递增作为覆盖序列。Doris 查询时 SUM 分片，避免在 Flink 再把全站汇聚成一个热 key。
- 金额累计不使用“几分钟无活动就 TTL 清零”。每个目标日绝对关闭时间为 `日期+online.metric.days+1` 的零点；到期释放状态，后来落到已关闭日的增量进入 repair，绝不从 0 再覆盖历史结果。
- 输入默认允许事件日以及其后 7 个日历日，即 `日期+8天零点` 关闭；向未来超 5 分钟进 quality。eventId/日 UV/退款订单去重状态 TTL=9 天，处理时间 TTL 配合绝对准入门，避免 TTL 后旧事件再次计入。物理回收依赖 RocksDB compaction，不代表到期瞬间磁盘立即归零。
- 指标日保留 93 天用于近期原支付日退款修正。今天收到 100 天前支付订单的退款，今天退款额可以更新，但原支付日净额修正会进入 repair。未补算前历史净额不是最终对账值。
- 当前不是窗口计算，**没有使用 Watermark 把消息等待几秒排序**。合法事实按金额可交换增量计算，不同事件之间可乱序；短暂负净额或多视图更新不同步应允许，最终以对账收敛。业务 ID 和历史维度快照必须稳定。

金额和去重Job在DWD入口均检查7日准入范围，再展开维度。金额状态保留93日用于“近期退款回扣历史支付日”，不表示可以接纳任意93日以前产生的事件。审计Job保留合法历史事实，不受在线指标7日准入限制。各Job在不同时刻消费，仍可能在日期边界产生不同进度，必须按repair记录和共同截止点对账；这不是跨Job原子事务。

Flink checkpoint 保存 Kafka offsets、去重、精确计数与分片累计。CDC→Kafka、事实→Kafka和指标→Doris均启用checkpoint协调的事务提交。查询可能短暂看到审计与汇总的不同进度，需同时观察checkpoint与业务对账。

**无状态启动不能继续覆盖已有累计表。** Kafka保留14天且线上接纳7日迟到，无法重建全部93日状态；update_seq也会重新开始。正常升级恢复兼容checkpoint/savepoint；彻底重建使用独立命名空间和完整历史，在明确截止点对账后切换查询。

业务日期、shard 数、key 的组成、state descriptor、序列语义、计数数组字段顺序都是状态契约。变更时需要状态迁移或隔离重建。parallelism 在 maxParallelism 和序列化兼容前提下可以通过恢复调整；已有恢复测试不等于已验证所有扩缩容组合。

## 8. 代码分层

阅读代码建议按以下顺序，不需要从所有工具类开始：

1. `CommerceFactJob`、`CommerceAmountMetricsJob`、`CommerceDistinctMetricsJob`：查看输入输出和作业职责；`TradePipelines`连接共同处理步骤。
2. `EventExpansion.expand`：看下单、支付、退款如何变成命名指标；`Metric` 给出固定状态下标与 Doris 字段的映射。
3. `AdmitAndDeduplicate`、`ExactDistinctFunction`、`ShardedAccumulator`：看重放处理、人数去重和日分片累计。
4. `BusinessGenerator.generate`：按创建订单 → 支付 → 退款阅读模拟业务；每个阶段返回同事务提交的业务行与 Outbox。
5. `CommerceMySqlSimulator`、`CommerceKafkaSimulator`：最后看共用交易计划如何分别写入 MySQL 或 Kafka，以及速率控制和发送确认。

入口负责组装拓扑，`ExpandTradeMetrics`负责事实到增量的转换，状态算子负责幂等及累计。算子UID、状态descriptor、金额数组布局与分片规则均属于恢复契约；`ReadabilityRegressionTest`固定模拟交易输出和指标布局。

| 包 | 职责 |
|---|---|
| config / common | 每个 Job 的配置边界、金额/字段约定、JSON 基础设施 |
| model / validation | 事件、指标增量、入站契约 |
| mock | 纯业务场景生成、每业务动作的事务计划、JDBC 写入、可控速率入口 |
| source | MySQL CDC、Kafka、Doris 工厂；地址/topic/表名在配置，不在业务逻辑硬编码 |
| function | 解析、准入去重、维度扩展、精确去重、分片累计、维度版本处理 |
| job | 六个职责独立的作业；环境、配置校验与checkpoint统一创建 |
| ops | 显式建 Topic、容量计算、纯 CPU 微基准 |

模型为 Flink/JSON 边界对象，保留简单字段结构以降低状态序列化复杂度；内部金额规则独立可测试。没有把每个类包装成无实际用途的 repository/service/interface 层。未来事件 schemaVersion 与状态迁移必须显式管理，不能只靠普通 getter/setter 保证兼容。

## 9. 当前明确没有完成的生产能力

真实渠道回调鉴权、分布式退款额度占用、跨库交易事实协调、拆合支付、赠品零元行/组合商品、运费退款、多币种、分片业务库采集、历史维度 SCD2、长期湖仓归档、自动补算发布、监控平台告警接入、压测集群验收均未实现。对缺失能力应按本企业业务选做，不能把示例称为整套电商交易平台。

零元商品行当前契约会拒绝，因此涉及赠品/零元支付的真实订单不能原样接入；需要先扩展商品数量与金额分离的契约及测试。金融事实错误或发生日期更正不能修改原 Outbox；需设计单独受审计的冲正/修正事件或离线重建，当前四类事件不包含通用更正。

参考依据： [Flink CDC 3.2 MySQL connector](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.2/docs/connectors/flink-sources/mysql-cdc/)、[Flink 1.19 状态后端](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/ops/state/state_backends/)、[Doris Unique Key 模型](https://doris.apache.org/docs/2.1/table-design/data-model/unique/)。业务口径是本模块明确选择的设计，不声称这些组件文档规定了行业统一 GMV 定义。

Outbox的唯一事件ID、按订单聚合键分区及归档删除语义参考 [Debezium Outbox Event Router](https://debezium.io/documentation/reference/transformations/outbox-event-router.html)。Flink状态与恢复遵循 [Flink 1.19 Working with State](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/fault-tolerance/state/)。
