# LinyuReal：电商交易事实与多维 GMV（Flink 1.19）

新 Java 代码全部在 `org.linyureal`，不改变原 `org.linyu` 任务。这里实现的是有明确边界的教学/联调模块，不宣称已具备生产上线条件。

## 1. 业务边界与口径

采用**订单表 + 支付成功流水表**分离模式。支付失败可以记录在支付服务的支付尝试表、渠道交互日志中，但不进入本模块 `trade_payment` 成功资金流水。不是所有公司的“支付表”都只记录成功，必须按实际表契约判断。

- 单商家子订单、人民币、一次全额支付；不支持合并支付、组合支付、分期、预售尾款、多币种。
- 商品实付金额已经由交易服务分摊优惠；不在 Flink 用商品标价重新推算。
- 订单实付 = 商品实付合计 + 运费。GMV **不含运费**。
- 每个退款单的 `amount_cent` 是**本次**商品退款额，不是订单累计退款额。支持一个订单多次部分退款。
- `FULL_REFUND` 场景是商品金额全部退清，不退运费。需要退运费时，生产应增加独立运费退款分项，本版不支持。
- 金额统一 `BIGINT` 分，Java `long` 精确运算且溢出报错；落 Doris 汇总时转 `DECIMAL(20,2)` 元。
- 支付/退款成功时间来自服务确认的业务时间，不用 Kafka 消息时间、CDC `ts_ms` 或 Flink 处理时间代替。
- 本版 DATETIME 契约为 Asia/Shanghai 墙上时间。跨时区业务应明确 UTC 时间戳与业务时区转换规则。

| 指标 | 归属日期 | 含义 |
|---|---|---|
| paid_gmv | 原支付日 | 成功支付的商品金额，不因退款减少 |
| refund_amount | 退款成功日 | 当日实际成功的商品退款金额 |
| net_paid_gmv | 原支付日 | 原支付商品金额减去后续所有成功退款 |

例如 8 月 11 日支付 300 元，12 日退 30 元：11 日 paid=300、net=270；12 日 refund=30。**跨日情况下同一行的 net 不等于 paid-refund**。需要现金流净额可另算当日 paid-refund，不应混用名称。

## 2. 表与生产服务职责

DDL 在 `mysql/schema.sql`。

| 表 | 粒度/主键 | 业务维护逻辑 |
|---|---|---|
| trade_order | 商家子订单/id | CREATED→PAID 或 CANCELLED；可 COMPLETED；同事务创建订单和明细 |
| trade_order_item | 订单商品行/id | 保存 SKU、SPU、品类、品牌、店铺及优惠后实付快照，交易成立后不改 |
| trade_payment | 成功支付流水/id | 校验渠道签名及金额，成功后插入；渠道流水唯一、本版订单唯一；重复回调幂等 |
| trade_payment_item | 支付商品分摊行/id | 与成功流水同事务保存，每商品行唯一；商品分摊和 + 运费 = 支付流水金额 |
| trade_refund | 一次退款申请/id | APPLIED→PROCESSING→SUCCEEDED/FAILED/CLOSED；只有确认成功才写 refund_time |
| trade_refund_item | 退款商品分项/id | 每退款单每商品行一条，金额和等于退款头；成功后不可改 |

示例 MySQL 模拟器把支付流水、分摊、订单 PAID 更新放在一个本地事务。真实微服务如果订单和支付不在同库，**不能假设跨服务本地事务**；由支付服务持久化成功事实，配合事务 Outbox、可靠消息、幂等消费、补偿与对账更新订单服务。Flink 消费各自 CDC 后允许它们先后到达，不负责执行支付或推动订单服务状态。

实际服务还必须实现：退款额度并发锁定/预占、成功回调幂等、失败释放预占、渠道对账、审计与权限控制。退款成功后累计退款不得大于可退商品实付。SQL 的外键和检查不是跨行并发金额约束，不能替代业务事务。本版 mock 单线程生成合法数据，不是完整支付系统。

每一行更新都必须在同一个数据库事务中 `version=version+1`，同主键版本单调递增；不拿时间戳做版本。`order_id`、支付资金流水和已发布事实不可修改；财务纠错走审计冲正/重建方案，不偷偷覆盖历史行。本版遇到冲突进入 DLQ，不自动猜测修正金额。

## 3. 链路与包结构

```text
Kafka 模拟器 ───────────────────────────────┐
MySQL 模拟器/真实服务 → MySQL binlog → CDC ─┴→ 6 个 ODS Topic
  → TradeFactJob：解析/校验 → keyBy(order_id) → 关联及版本去重
  → 商品支付事实 Topic + 商品退款事实 Topic
  → DimensionGmvJob：按 fact_id 去重 → 展开维度 → 按日期/维度累计
  → Doris 明细事实 + 日汇总

商品/类目/店铺/品牌/行政区主数据 → CDC/维度模拟 → 6 个维度 Topic
  → DimensionSyncJob → Doris dim_catalog → 查询时补充展示名称
```

- `config/common`：配置、JSON、异常输出。
- `model`：CDC 输入、交易状态、事实与指标模型。
- `validation/parser`：输入契约和 CDC 适配。
- `function/trade`：纯业务关联器及 Flink 状态适配。
- `function/metric`：事实去重、维度展开、日汇总、维度版本控制。
- `source/sink/job`：连接器工厂与三个作业入口。
- `mock/generator/scenario/writer`：场景、数据生成及 Kafka/MySQL 写入。

订单明细数、支付分摊数达到订单声明的 item_count，金额校验通过后才产生支付事实。成功退款需收到全部声明明细才产生退款事实。单个不完整退款不会阻塞其他已完成退款。跨表跨 Topic 的消息到达顺序不保证一致，因此保留中间状态等待，旧版本忽略，同版本相同内容去重，冲突送 DLQ。

支付事实 ID=`PAYMENT:支付分摊ID`，退款事实 ID=`REFUND:退款分项ID`。第二个 Job 再按事实 ID 去重，防止重复累计。候选状态先校验，成功后才提交，避免错误消息污染已有正确状态。

没有事件时间窗口，也就不靠 Watermark 关闭某一天；日期从业务成功时间取日期部分，迟到退款能修改原支付日净额。处理时间定时器只合并输出频率。

## 4. 维度设计和存储

生产商品中心维护 SPU/SKU，类目中心维护类目树，商家中心维护店铺，品牌中心维护品牌，行政区划字典维护省市区。业务主存通常是关系数据库/主数据系统，本例合并放到 MySQL `dim_*` 便于演示，不意味着生产必须共享一个库。

- 商品维度按 SPU `product_id` 汇总；保留 SKU 用于追溯，未额外输出 SKU 汇总。
- 品类取交易时的叶子类目；不自动把金额重复汇总到所有祖先类目。
- 店铺、品牌使用交易时的 ID。
- 地区使用下单收货地址快照，按 PROVINCE、CITY、DISTRICT 分开，不混成一种地区粒度。
- 数量与金额的归属用订单行快照，而非读取商品今天归属的品牌/类目重算历史。
- Doris `dim_catalog` 保存最新展示名称（SCD Type 1），按 version 防止旧名称覆盖新名称。改名不影响历史金额归属。如果要查询交易当时名称，需要 SCD Type 2 或扩展订单名称快照，本版没有实现。

维度晚到时仍保留指标，用 LEFT JOIN 名称，不丢金额。品类树变更的历史口径、地址变更是否追溯是业务定义，不自动重算。**不同维度类型不能直接 SUM 在一起**，否则同笔金额重复计算七次。

## 5. Kafka 快速联调（不需要 MySQL）

以下为待操作步骤，代码生成过程不会执行它们。先检查地址和独立命名空间，避免写到原来的 GMV Topic。

1. 在 Doris 执行 `doris/schema.sql`，默认一副本仅用于开发。
2. 调整 `application.properties`，或新建外部 properties 覆盖相应键。配置通过第一个程序参数传入；Topic 均从配置读取。
3. 打包并显式创建 Topic：

```bash
mvn test package
java -cp target/flink-datastream-job-all.jar org.linyureal.mock.TopicSetupMain /absolute/path/local.properties
```

默认 Kafka 地址 localhost:9094、Topic 3 分区1副本。TopicSetup 只创建缺失 Topic，不修改既有配置。数据 Topic 演示保留期 `-1`；生产要结合归档与重建基线制定有限保留期。

4. 给 Flink JobManager/TaskManager 运行环境配置 `LINYUREAL_DORIS_PASSWORD`（空密码也需显式设置为空）。提交这三个入口，使用 Flink 1.19.3：

```text
org.linyureal.job.TradeFactJob
org.linyureal.job.DimensionGmvJob
org.linyureal.job.DimensionSyncJob
```

Web UI 上传上述 jar，分别填写 Entry Class。Program Arguments 填配置文件绝对路径，**该文件必须存在于执行 main 的集群环境中**，不是浏览器电脑上的路径。容器可参考 `cluster.properties` 并挂载文件。三个作业需要足够 slot，1 并行度至少预留3个 slot。

5. 运行模拟器（这是实际写入命令）：

```bash
java -cp target/flink-datastream-job-all.jar org.linyureal.mock.TradeMockMain /absolute/path/local.properties
```

默认 Kafka 模式、80 个订单、目标每秒5个订单；**不是每秒5条 Kafka 消息**。每订单有多个生命周期消息且可能重复，写入耗时会降低实际速率。`mock.duplicate.rate=0.10` 对每消息概率重发，`mock.shuffle.rate=0.20` 对每订单概率打乱跨表消息。不是随机制造非法业务状态。

场景按枚举循环：成功、取消、支付失败后重试成功、部分退款、多次退款、商品全退、退款失败、跨日退款。失败支付只体现在重试场景，没有成功前的 payment 行。所有生命周期快速发出，业务时间在当前日期前两天，跨日退款在前一天，**不是实时等待一天退款**。每批 mock.run.id 必须换新；复用是重放相同 ID，不会作为新订单累计，且跨天复用可触发冲突。

默认80订单=10轮。忽略重复/乱序且无脏数据时，每一个维度类型的总 paid=21000元、refund=4100元、net=16900元。明细事实共210条（支付140、退款70）。支付流水总21700元，差额700元为运费。商品全退场景不退运费。默认商品付款日在前两天，跨日退款日在前一天。

6. 执行 `doris/reconcile.sql`，观察 `lr.dlq.trade`，对照三个 Job checkpoint、Kafka read_committed lag、Doris Stream Load。运行状态 RUNNING/Checkpoint 成功不等于业务已产生输出。链末端 Records Sent=0 也不表示外部写入失败。

## 6. MySQL + 真 CDC 联调

此模式与直写 Kafka 模式二选一，不要把两种生产者混入同一批订单。

1. 可选启动 `cdc/docker-compose.yml`：需要已有 `flink-network` 以及网络名 `kafka:9092` 的 Kafka。设置 `LINYUREAL_MYSQL_PASSWORD` 后执行 `docker compose -f src/main/resources/linyureal/cdc/docker-compose.yml up -d`。这里不包括 Kafka、Flink、Doris。
2. 本例固定 MySQL 8.0.40 + Debezium 2.7（版本组合按官方支持范围选取）。MySQL 初始化只在空数据卷首次运行，已有实例由 DBA 执行 schema.sql；不要为了重跑删除已有数据卷。
3. MySQL 启用 ROW binlog、FULL row image、唯一 server-id，保留期覆盖最大停机时间。创建独立 CDC 用户，按当前快照方式授予最小所需权限，例如开发环境：

```sql
CREATE USER 'cdc_reader'@'%' IDENTIFIED BY 'REPLACE_WITH_STRONG_SECRET';
GRANT SELECT ON linyureal.* TO 'cdc_reader'@'%';
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_reader'@'%';
```

初始快照锁策略可能要求额外 `LOCK TABLES` 等权限，由 DBA 根据部署方式调整；生产限制来源网段、启用 TLS，不直接照抄开发账号。

4. 用 TopicSetup 创建数据 Topic。Kafka Connect 内部 config/offset/status Topic 要 compact 且按生产副本数配置；Debezium schema-history Topic 应**单分区、完整保留、不 compact**。开发可显式创建：

```bash
kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic lr.internal.schema-history --partitions 1 --replication-factor 1 --config cleanup.policy=delete --config retention.ms=-1
```

5. 复制 `cdc/mysql-connector.json` 到安全配置位置，替换密码/地址，提交到 Connect（不要把真实密码提交 git）：

```bash
curl -X POST -H 'Content-Type: application/json' --data-binary @/secure/path/mysql-connector.json http://localhost:8083/connectors
curl http://localhost:8083/connectors/linyureal-mysql/status
```

`snapshot.mode=initial` 首次读存量，再读 binlog；重启依赖 Connect offsets 和 schema history。路由 `lr-source.linyureal.<table>`→`lr.ods.<table>`。如果修改 Topic 配置，也要同步修改 connector 路由，TopicSetup 不会替你修改 CDC。

`time.precision.mode=connect` 对本例 DATETIME(3) 输出毫秒逻辑值，解析器按无时区墙上时间恢复后归日。不要把 adaptive 模式的微秒误当毫秒；也不要改成 TIMESTAMP 后继续沿用相同解析规则。没有使用 SMT 解包，after/before/source/op 信封要保留。

6. 外部配置设 `mock.mode=mysql`、mysql.url、mysql.username、新的 mock.run.id；环境变量设 LINYUREAL_MYSQL_PASSWORD，运行同一个 TradeMockMain。它按事务写六张业务表，不直接发 Kafka；维度由 schema.sql 初始化，CDC 同步。Kafka 模式的重复/乱序参数在 MySQL 模式不生效，避免制造错误数据库历史。
7. 确认 Connect RUNNING、初始快照完成、ODS 有 c/u/r 事件，再验证三个 Flink Job 与 Doris。不要用删除 Kafka offsets 或任意跳过 binlog 错误来“恢复”；先评估缺口并重做完整基线。

CDC 只负责搬运变化，不推断支付成功，不补齐跨服务事务。数据开发负责识别成功事实、金额与引用校验、乱序关联、幂等、对账和告警；不能把业务服务的未知支付状态判为失败，也不能替业务批准退款。

参考：[Debezium 2.7 MySQL 文档](https://debezium.io/documentation/reference/2.7/connectors/mysql.html)、[2.7 支持范围](https://debezium.io/releases/2.7/)。

## 7. 一致性、恢复及上线前必做

- Kafka Sink exactly-once，Source read_committed；Doris 2PC 随 checkpoint 提交。第二个 Job 聚合输出5秒再等 checkpoint，端到端延迟可能叠加两个 Job checkpoint，并非固定5秒。
- Kafka broker 的 transaction.max.timeout.ms 应至少覆盖配置的600000ms；单 broker 测试环境事务状态日志副本/min ISR需设1，生产按高可用要求配置。
- Kafka transactional prefix、Doris label prefix 对同时运行的独立实例必须唯一；恢复同一作业保持标识与 operator uid。不得同时启动两个实例写同一汇总表。
- 三个作业、Kafka 多 Topic 与 Doris 两张表**不是跨系统全局原子事务**；退款先于支付到达下游可暂时出现负净额，最终收齐后一致。查询与对账需考虑未完成 checkpoint，不能要求每个瞬间不同表都相等。
- checkpoint 默认本地目录仅开发可用，生产需共享持久化存储。已开启取消时保留 external checkpoint，但必须验证路径真实可访问。Web UI 重提时填已完成 checkpoint/savepoint 的完整路径；不要启用“忽略无法恢复状态”来绕过兼容性错误。
- 从无状态 earliest 重跑依赖完整 ODS/DWD 历史。Kafka 历史已过期时必须建立新快照/归档重建流程；不能用仅剩增量覆盖现有 Doris 总额。受控重建应写新表并对账后切换，不能边覆盖旧表边对外服务。
- 本版资金状态与去重状态**不配置自动 TTL**，避免长周期退款或重放因状态过期重复计费。代价是状态持续增长；生产必须定义售后/对账关闭周期、归档基线、补算与去重保留策略后再做清理，不能直接加7天 TTL。
- 关联超时发 DLQ 并保留状态等待迟到；它是数据质量事件，不使整个任务失败。必须建设 DLQ 告警、按订单回查六表、修正/重放操作及每日资金对账。目前没有自动告警服务。
- 不支持对成功事实修改/删除的自动冲正，也不支持历史维度 ID 纠正自动重分配。需要这些业务时扩展版本化冲正事件，不可直接修改源表绕过契约。
- 生产还需容量压测、状态规模评估、checkpoint故障恢复演练、Kafka/Doris事务故障测试、服务权限/TLS/SASL、监控告警与数据隐私脱敏。本版连接器只覆盖当前开发环境的基础认证配置，不宣称通用于生产安全集群。

## 8. 自动化验证

```bash
mvn -Dtest='org.linyureal.*Test' test
mvn test package
```

测试覆盖8个业务场景、400组乱序/重复排列、等待缺失分摊、同版本冲突、超额退款、跨日指标口径、业务状态序列化重放以及本地 Flink 1.19 解析/关联/去重链路。业务状态 JSON 重放测试不等于真实 checkpoint 恢复演练。外部 Kafka/MySQL/Doris 和完整 CDC 链路需要按本说明联调，单元测试不连接它们。
