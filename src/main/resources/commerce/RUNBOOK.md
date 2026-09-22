# 本地联调、配置、恢复与补算

发布前准备业务数据库、消息队列、结果库和共享Checkpoint存储。以下命令按开发环境给出，生产由发布平台注入网络地址、凭据与资源配置。

## 1. 编译与测试

从工程根目录执行，使用JDK11及Flink1.19.3匹配的运行环境：

```bash
mvn -q -Dtest='org.commerce.*Test' test
mvn -q package
java -cp target/flink-datastream-job-all.jar org.commerce.ops.CapacityCalculator
```

输出fat jar：`target/flink-datastream-job-all.jar`。Flink运行时依赖为provided，由集群提供；不要把Flink核心多版本混入集群lib。Doris connector使用工程中的`flink-doris-connector-1.19:25.1.0`；真实Doris服务器版本尚未确认，需要用实际版本验收DDL、Sequence列和Stream Load 2PC行为。MySQL CDC3.2.1支持Flink1.19，升级其他组合请查其兼容矩阵。

## 2. 配置与秘密

每个入口先加载jar内 `/commerce/application.properties`，可传**第一个参数**作为外部properties文件路径，覆盖默认值。配置路径不是`--config path`格式，也不是URL。

- 本地默认MySQL localhost:3308、Kafka localhost:9094、Doris FE localhost:8030；实际地址按自己的环境改。
- 所有Topic来自`topic.*`，所有sink表名来自`doris.table.*`；生产候选覆盖示例见`production-candidate.properties`，`REPLACE_*`必须先替换。
- 密码只从`COMMERCE_MYSQL_PASSWORD`、`COMMERCE_CDC_PASSWORD`、`COMMERCE_DORIS_PASSWORD`环境变量读取，不写进git。变量不存在会直接报错；Doris开发环境如确实空密码，也要显式设置空变量。
- 集群/WebUI提交时，JobManager执行入口，需要能读取外部配置路径和环境变量；按部署方式将配置、secret挂载到正确容器，不要只在自己的Mac终端export。
- `s3://...` checkpoint路径需要集群预装相应Flink filesystem插件和最小权限凭据；对象存储目录必须隔离且可持续访问。
- `deployment.environment=production`启动时拒绝本地Checkpoint目录、未替换的REPLACE占位符、Kafka副本小于3/minISR小于2。该检查只校验配置，实际Topic配置、共享存储可用性仍需平台确认。
- `kafka.transaction.timeout.ms`必须大于“该Job的CK周期 + CK超时 + recovery.budget.ms”；恢复预算是运维约定，并非程序承诺的恢复时间。Job及Doris并行度不得超过maxParallelism。
- `pipeline.mode=split`运行六个独立作业；`integrated`才允许提交CommerceMetricsJob。部署平台还需禁止同一输出表同时被两个独立实例维护。
- 当前connector工厂面向隔离内网演示；Kafka SASL/TLS、MySQL TLS、Doris TLS、secret轮换、RBAC与网络隔离需要按企业平台补齐并验收，不能用明文示例直接暴露公网。

Topic事务prefix、CDC server-id、consumer group、checkpoint目录、Doris label必须按并行部署的逻辑作业隔离；恢复同一逻辑作业保留相容配置。不要让两份独立Job写同一分片累计表。

## 3. 初始化MySQL

可在已有独立MySQL8实例执行`sql/mysql.sql`；或使用附带compose创建端口3308的实验实例（首次初始化挂载SQL）：

```bash
docker compose -f src/main/resources/commerce/mysql-compose.yml up -d
```

先通过自己的秘密管理方式设置`COMMERCE_MYSQL_PASSWORD`。compose使用持久volume，再次启动不会重新执行建表脚本；`CREATE TABLE IF NOT EXISTS`也不会迁移旧表。这个新模块的DDL不应拿去覆盖旧业务库。

MySQL必须启用唯一server-id、ROW binlog和FULL row image。示例binlog保留14天，需要按最大CDC停机/回补时长调整并测磁盘。示意CDC授权如下，由DBA替换账号host和密码并执行，不建议生产使用`%`：

```sql
CREATE USER 'commerce_cdc'@'<FLINK_NETWORK_HOST>' IDENTIFIED BY '<SECRET_FROM_VAULT>';
GRANT SELECT, LOCK TABLES ON commerce.* TO 'commerce_cdc'@'<FLINK_NETWORK_HOST>';
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.*
TO 'commerce_cdc'@'<FLINK_NETWORK_HOST>';
```

生产数据库权限应依据Flink CDC实际snapshot模式核对最小授权。模拟器需业务表INSERT/UPDATE和维度SELECT/INSERT权限；线上真正Outbox写账号尽量禁止UPDATE/DELETE。不要把模拟器root账号当生产接入规范。

## 4. 初始化Kafka与Doris

显式创建Topic：

```bash
java -cp target/flink-datastream-job-all.jar org.commerce.ops.CreateTopics
```

如有外部配置，将其路径追加在最后。工具只创建不存在的Topic，已存在时仅提示，不修改分区、retention或副本。请核对已有Topic实际配置：outbox保留14天；六个维度compact；quality/repair保留90天。生产Kafka应3副本、minISR2，broker的最大事务超时必须容纳客户端900000ms，恢复最长停机也要纳入事务超时规划。

在Doris SQL客户端依次执行`sql/doris.sql`和`sql/doris-split.sql`，查询`commerce.dashboard_v2`。本地默认replication=1；生产改成3并核对BE数、动态分区配置和分桶。创建后确认当前日及需修正历史日分区存在。

**DDL动态分区策略会淘汰约120天前的分区，属于实际的数据删除策略**；只适用于已归档且接受该在线保留期的环境。没有配置自动归档任务。生产建表前由数据所有者确定保留期并修改，不得把在线120天当成全年财务留存。手动删数据后无法靠Flink仍在运行自动全量补回。

## 5. 启动作业

六个入口（WebUI分别填入对应Entry Class）：

```text
org.commerce.job.CommerceCdcJob
org.commerce.job.CommerceFactJob
org.commerce.job.CommerceAmountMetricsJob
org.commerce.job.CommerceDistinctMetricsJob
org.commerce.job.CommerceAuditJob
org.commerce.job.CommerceDimensionJob
```

CLI示例：

```bash
flink run -d -c org.commerce.job.CommerceCdcJob target/flink-datastream-job-all.jar
flink run -d -c org.commerce.job.CommerceDimensionJob target/flink-datastream-job-all.jar
flink run -d -c org.commerce.job.CommerceFactJob target/flink-datastream-job-all.jar
flink run -d -c org.commerce.job.CommerceAmountMetricsJob target/flink-datastream-job-all.jar
flink run -d -c org.commerce.job.CommerceDistinctMetricsJob target/flink-datastream-job-all.jar
flink run -d -c org.commerce.job.CommerceAuditJob target/flink-datastream-job-all.jar
```

WebUI：上传jar，每个入口分别提交一个Job；Program Arguments可填JobManager可访问的外部properties绝对路径。并行度由代码读取各Job配置设置，不依赖WebUI输入框覆盖。

CDC首次执行initial snapshot后切binlog，只读取`trade_outbox`和六张维度表；不会把全部业务表原始CDC直接发到Kafka。指标需要的是原子业务事实，不是七张业务表的模糊关联结果。source恢复时使用checkpoint中的进度。Kafka指标source全新启动使用earliest，恢复时用checkpoint记录offset，不能拿consumer group成员数是否0判断source是否活着。

等待六个Job正常RUNNING且有完成checkpoint，再开始短批次模拟，检查真实入库。正常情况下能消费并不代表一定有成功支付事件；应核对quality、repair及每阶段数量。

## 6. 模拟业务与控速

```bash
java -cp target/flink-datastream-job-all.jar org.commerce.mock.CommerceMySqlSimulator
```

参数均在配置，示例覆盖文件内容：

```properties
mock.run.id=review001
mock.orders=1000
mock.orders.per.second=10
mock.workers=2
mock.scenario=PROFILE
# 两个模拟入口需要逐条比对时填写相同的固定业务起始时间；普通联调可省略。
# mock.start.time.ms=1789956000000
business.payment.rate=0.20
business.average.paid.cent=2740
```

- `orders.per.second` 是整个进程的**创建订单启动速率**，不是每线程速率，也不是CDC消息速率。一订单拆多个事务/Outbox；JDBC瓶颈时实际速率更低，结尾会打印实测值。并发线程共用全局节拍，数据库变慢后可能短时追赶积压许可；它不是严格无突发的API限流器。
- `mock.orders`有限，默认1000；不会无限向生产灌数据。`run.id`每批换一个，避免业务主键碰撞；不会遇到重复ID就修改旧订单。
- `PROFILE`按容量假设分布；也可以指定`PAID`、`CANCELLED`、`PAYMENT_FAILED`、`RETRY_PAID`、`PARTIAL_REFUND`、`MULTIPLE_REFUNDS`、`FULL_GOODS_REFUND`、`REFUND_FAILED`。
- `MATRIX`轮流九场景，含`OLD_ORDER_REFUND`。该旧单场景把14天前创建/支付和今天退款一起插入，用来**演示历史引导/修复边界**：线上7日准入会将旧创建/支付送repair。若没有预先重建原支付基线，不能拿它验证“历史净额自动正确”。正常退款回溯测试先准备完整支付历史/恢复状态，再发送退款事实。
- 为快速联调，业务时间在当前前5分钟，支付/退款/取消按30–60秒逻辑间隔生成，但写入时不真睡30分钟。这是压缩时间的仿真；失败支付订单可以停在待支付阶段，未模拟完整超时关单服务。
- 维度初始化仅插入缺少的数据；已有同ID但不同内容会失败，避免偷偷覆盖用户维度。生产主数据应由真实业务系统维护，不要运行样例初始化器。

### 独立运行的 Kafka 模拟任务

先按第4节创建配置中的 ODS、DWD、质量和补算 Topic。只启动 `CommerceFactJob`、`CommerceAmountMetricsJob`、`CommerceDistinctMetricsJob`、`CommerceAuditJob`、`CommerceDimensionJob`，不启动 `CommerceCdcJob`。随后运行：

```bash
java -cp target/flink-datastream-job-all.jar org.commerce.mock.CommerceKafkaSimulator
```

同样可以把外部 properties 文件路径作为最后一个参数。此入口读取与 MySQL 模拟器相同的 `mock.run.id`、`mock.orders`、`mock.orders.per.second`、`mock.workers`、`mock.seed`、`mock.scenario`、支付率和平均商品金额；Kafka 地址及 Topic 读取 `kafka.bootstrap.servers` 和各项 `topic.*`。它不创建 JDBC 连接，不读取 MySQL 密码，无需运行 MySQL 服务。

两个模拟入口是二选一的独立进程，不要求先运行 MySQL 模拟器。想逐条比较两种路径时，用同一份覆盖文件分别运行，增加例如 `mock.start.time.ms=1789956000000`（2026-09-21 10:00:00，Asia/Shanghai）。该值是第 0 单的创建时刻，后续订单按 `订单序号 / mock.orders.per.second` 推进业务毫秒时间；随机种子、`run.id`、订单数、场景和金额参数也要相同。这样业务表行与 Outbox 事件在两次运行中相同；Kafka CDC envelope 的 binlog 元数据不是模拟对象。未设置固定时间时保留原来的“当前时间前5分钟”规则，两次分别运行的时间戳不会完全一致。固定时间用于在线指标测试时应选择当前准入范围内的日期，否则旧事件会按业务规则进入 repair。

模拟器先发送同一份维度目录，再按订单生成完整交易计划，只将计划中的 Outbox 事件包装成下游可识别的 CDC 消息发到 `topic.trade_outbox`。消息 key 为订单 ID；维度消息 key 为维度 ID。一个订单内的创建、支付、退款事件按计划顺序发送并等待 Kafka 确认；失败支付和失败退款没有成功金额事实。业务表行虽然仍由共享生成器构造，快速模式不向 Kafka 发布这些下游不消费的表，也不持久化到 MySQL。因此此模式验证事实、指标、审计和维度链路，不能替代 MySQL 事务与 CDC 联调。

使用独立测试 Topic/消费组或空的测试环境，避免与真实 CDC Job 同时向一套 ODS Topic 写入相同 `run.id` 的事件；再次生成测试批次时更换 `mock.run.id`。`read_committed` 消费者可以读取模拟器发布的普通 Kafka 记录。每条写入等待 broker 确认，程序异常会停止并报告错误，已成功写入的记录不会被自动撤销。首次启动后如果下游没有数据，应依次检查 ODS Topic、事实 Job 的 quality/repair、DWD Topic、金额/去重 Job 的 quality/repair 和 Doris 导入状态。

## 7. 对账与故障定位

先执行`sql/reconcile.sql`中的Doris部分，MySQL独立核验SQL已用注释分隔，按会话`+08:00`时间执行。要等CDC和sink追平，再以同一截止点对账。

检查链：

1. MySQL有无pay_ledger成功流水？refund_request是否真的SUCCEEDED？对应Outbox是否存在且金额守恒？
2. Kafka配置Topic是否一致？CDC checkpoint提交后`read_committed`能否看到事实？
3. quality有无契约/版本冲突？repair有无事件时限、指标目标日关闭？
4. 指标Job接纳/去重/分片输入是否增加？checkpoint有没有持续完成？
5. Doris表是否为配置目标、分区是否存在、Stream Load成功/提交/可见状态及错误日志如何？

重点监控`accepted_events`、`duplicate_events`、`conflicting_events`、`future_events`、`repair_events`，以及DWD消费者的`accepted_facts`、`invalid_facts`、`expired_facts`。`invalid_outbox_events`用于契约异常；`outbox_cleanup_deletes`用于核对归档清理量。计数器按子任务聚合，重启后重新计数，不可替代持久业务对账。

默认Doris strict_mode=true且max_filter_ratio=0，导入脏数据应报错而不是容忍过滤。链路质量问题则按显式side output处理，不会故意使整个Job失败；监控quality/repair是运行必要条件。

两种net指标不能混淆：原支付日net_paid_gmv与当天goods_cash_net。跨维度成员的订单/UV不可求和核对ALL，金额则应按同一种维度分别守恒。

## 8. WebUI取消后的恢复

本模块开启外部checkpoint且取消保留，路径分Job隔离。取消前记录最近一次成功checkpoint的外部完整路径/metadata；生产路径必须是共享持久存储，本地`file:///tmp`不适合跨机器恢复。

重新上传兼容jar，选择对应Entry Class，在WebUI的Savepoint Path/恢复路径输入该checkpoint路径（实际表单名称以Flink1.19页面为准），并填原外部配置。checkpoint与savepoint都不是Job ID。已取消但没有保存且存储不存在的状态无法凭空恢复。

不要勾选allowNonRestoredState来掩盖金融累计状态不兼容；保留UID、keyBy结构、state descriptor、序列化兼容、日期与分片规则。恢复失败先分析原因。新代码需要迁移时，应经过隔离恢复测试，而不是直接从earliest向旧Doris表重放。

## 9. 超期事件与历史修复

`repair`包含来源作业`job`、阶段`stage`、原数据`raw`、原因`reason`和观察时间`observed_at`。共享Topic中应按`job`区分金额、去重或事实链路，不能把一条修复记录无差别重放到所有指标：

- event-horizon：整条事实未接纳，不能直接把时间改成今天再投递；必须找到正确业务日和权威记录。
- fact-horizon：某个DWD消费者积压超过在线准入范围，整条事实在该Job未入账；需标明目标指标组，核对其他Job的完成进度后补算，不能向公共DWD直接重发。
- metric-horizon：某个历史目标日的单条维度增量未写；同一退款的当日退款腿可能已经成功，不能把整条退款直接再减一次。
- distinct阶段关闭：精确日计数需要独立重建；不能用近似补一条假装完成。

建议修复流程：登记工单→MySQL/长期归档确认权威事实→确定受影响日期/维度与已落账范围→暂停或建立明确水位边界→在隔离表/命名空间重建完整分片快照→金额与计数对账→受控切换查询→登记修复证据和结束位点。本仓库给出了repair通道及核验SQL，**没有实现自动执行这套历史补算的程序**。

Outbox、binlog、Kafka和Doris保留期必须独立治理：binlog14天保障CDC恢复；Kafka事实14天便于短期重放；线上准入7天限制重复计算范围；指标状态93天承担近期净额修正；Doris120天是当前示例在线查询保留。它们并不互相替代。归档Outbox并物理删除会产生CDC delete，解析器只增加`outbox_cleanup_deletes`计数，不发布交易或撤回金额。清理必须在归档校验完成且CDC读取越过对应记录后进行，并保留审计证据；计数器不能证明归档完整。

## 10. 变更上线检查

- 契约校验拒绝数字字符串、小数分值、缺失字段和重复JSON字段。切换前抽样核对真实Outbox及DWD，确认生产方满足契约，不能通过放宽校验掩盖金额错误。
- Kafka交易消息以订单ID作为key，维度以主键作为key。调整key或Topic分区数会改变记录分区，不能依赖切换前后的全局顺序；同一订单的事实在eventId去重时也可能重排，指标依靠不可变事实及增量运算而非到达顺序。
- 已有有状态算子UID、状态描述符和分片规则保持稳定，新增诊断标注为无状态Map。仍须使用实际Checkpoint在隔离环境验证恢复、位点连续性和Doris小计，不能把本地恢复测试当作任意生产快照兼容保证。
- 生产配置检查只校验客户端声明。Kafka实际副本/ISR与broker事务上限、持久存储权限、Flink恢复耗时和Doris事务配置仍需平台验收。

上线前按[CAPACITY.md](CAPACITY.md)验收性能、故障恢复、保留期、数据对账与修复闭环，再逐步接入真实流量。
