# 从零设计：交易域实时数仓（MySQL → Flink CDC → Kafka → Flink → Doris）

本次所有新 Java 类在 `org.linyureal.realtime`；不依赖旧 `org.linyureal` 的业务类。配置、SQL、Topic、数据库和状态命名空间均独立，不修改旧 `org.linyu` 任务，也不覆盖用户的 `linyureal/order.sql`。

## 1. 这次实现的业务范围

仅交易域：下单、取消未支付订单、支付尝试、成功支付、退款申请和成功/失败退款。商品、品类、品牌、店铺、行政区是交易分析使用的维度，不引入库存、物流、营销归因、推荐或其他数据域。

采用可以真实落地但有明确限制的业务模型：

- 一个订单代表**单店铺子订单**，一次全额成功支付；未实现购物车父单、跨店合并支付、混合支付、分期、预售尾款和多币种。
- `payment_attempt` 记录每次调用支付渠道的尝试，包含失败；重试创建新尝试 ID。
- `payment_ledger` 只保存成功资金流水；失败支付不能插入成功流水。支付成功必须依赖渠道验签、金额核对和幂等处理，不由 Flink 猜测。
- 退款失败有退款申请记录，但不产生成功退款事实、不扣减指标。失败退款不是成功退款后再加回来。
- 每个退款单金额是**本次申请金额**，不是订单累计退款额。多次退款累计不能超过商品行实付。
- 支付 GMV 使用**商品实付、不含运费**。退款指标也只含商品退款。`FULL_GOODS_REFUND` 表示商品金额全部退清；如存在运费，并不代表整个支付流水全部退款。运费退款需要独立资金分项，本版未实现。
- 优惠由交易服务按商品行分摊到分，Flink 不重新定价。模拟器把一个订单优惠券按金额比例分摊，最后一行接收舍入尾差，保证每一分守恒。
- `order_line.paid_cent` 是该行约定的应付/实付基准，即使未支付也有值；**不能仅凭该列存在就认定支付成功**。
- 金额使用 CNY 整数分、Java long、溢出检查；Doris 基础表继续保存整数分，查询视图转元。
- 订单行、支付流水、支付分摊和退款行在该契约中不可变；只允许头表状态推进。已发布事实如需纠正，必须另行设计审计冲正，不直接覆盖历史记录。

这些是本模块选定的业务契约，不是所有电商公司唯一的数据表设计。

## 2. 包与作业职责

```text
org.linyureal.realtime
  common       表契约、JSON/时间处理
  config       独立的配置加载
  model        跨算子 POJO、订单状态、事实和指标输入
  validation   源表字段和业务约束
  parser       CDC、交易事实、维度消息解析
  mock         维度字典、业务事务生成、MySQL 写入、Topic 创建工具
  source       Flink CDC Source、Kafka Source/Sink 工厂
  sink         CDC 按表路由、Doris Sink 工厂
  function     交易投影、事实去重、维度展开、日级指标计算
  job          四个 Flink 作业入口
```

`MySqlSimulator` 是普通 Java 程序，不是 Flink Job，不直接发 Kafka。`CreateTopics` 是显式部署工具，只创建缺失 Topic，不删 Topic、不修改已有配置。

| 作业入口（均带 org.linyureal.realtime.job 前缀） | 输入 | 输出 |
|---|---|---|
| MysqlCdcToKafkaJob | MySQL 7 张交易表、6 张维度表 | 13 个 ODS Kafka Topic |
| TradeFactJob | 7 个交易 ODS Topic | 1 个交易 DWD Topic、异常 Topic |
| TradeMetricsJob | 交易 DWD Topic | Doris 交易明细事实表、日指标表、异常 Topic |
| DimensionSyncJob | 6 个维度 ODS Topic | Doris 最新维度目录、异常 Topic |

## 3. 第一层：MySQL 业务表结构

完整字段类型、主键、唯一键和约束见 `sql/mysql.sql`，独立数据库 **rt_trade**。

所有交易表共有：

| 字段 | 含义 |
|---|---|
| id | 本表业务主键，VARCHAR(64) |
| order_id | 所属子订单 ID；Flink 跨表关联键，不一定等于本表主键 |
| version | 单行单调递增版本；每次更新在同一事务中 +1 |
| updated_at | 数据行更新时间，DATETIME(3)；不用它代替版本或业务日期 |

各表独有字段和粒度：

| 表 | 一行代表什么 | 主要字段 |
|---|---|---|
| order_header | 一个单店铺子订单 | user_id、shop_id、province_id/city_id/district_id、currency、status、created_at、cancelled_at、line_count、goods_cent、freight_cent、payable_cent |
| order_line | 一个订单商品行 | sku_id、product_id、category_id、brand_id、shop_id、quantity、unit_price_cent、discount_cent、paid_cent |
| payment_attempt | 一次支付尝试 | channel、status、amount_cent、failure_code |
| payment_ledger | 一次确认成功的支付 | attempt_id、channel_transaction_id、status=SUCCEEDED、amount_cent、paid_at |
| payment_line | 成功支付中一个商品行的资金分摊 | payment_id、order_line_id、amount_cent |
| refund_header | 一次退款请求 | payment_id、status、amount_cent、line_count、succeeded_at |
| refund_line | 一次退款请求的一个商品行退款额 | refund_id、order_line_id、amount_cent |

`order_header.id=order_id`；payment_ledger 的 order_id、attempt_id、channel_transaction_id 分别唯一；payment_line 的 order_line_id 在本版一次全额支付约束下唯一；refund_line 的 `(refund_id,order_line_id)` 唯一。

金额关系：

```text
每行 paid_cent = quantity × unit_price_cent − discount_cent
order.goods_cent = Σ order_line.paid_cent
order.payable_cent = goods_cent + freight_cent
payment_ledger.amount_cent = order.payable_cent
Σ payment_line.amount_cent = order.goods_cent（不含运费）
refund_header.amount_cent = Σ 本退款单 refund_line.amount_cent
每订单商品行的 Σ 成功退款 amount_cent ≤ 原商品行 paid_cent
```

SQL CHECK/外键能验证部分规则，跨行汇总和并发退款额度仍要业务事务保证；Flink 再做防御性校验和异常隔离。

### 3.1 模拟器产生的事务顺序

| 事务 | 写入/更新 |
|---|---|
| T1 下单 | INSERT order_header(CREATED) 和全部 order_line |
| T2 支付请求 | INSERT payment_attempt(PENDING) |
| T3 失败（若发生） | UPDATE attempt→FAILED，写 failure_code；没有 payment_ledger |
| T4 重试（若发生） | INSERT 新 attempt(PENDING)，不是把失败流水改成功 |
| T5 支付确认 | 同事务 UPDATE attempt→SUCCEEDED、INSERT payment_ledger/payment_line、UPDATE order_header→PAID |
| T6 退款申请 | 同事务 INSERT refund_header(APPLIED) 和全部 refund_line |
| T7 请求渠道退款 | UPDATE refund_header→PROCESSING |
| T8 渠道确认 | UPDATE refund_header→SUCCEEDED 且写 succeeded_at，或→FAILED 且无成功时间 |
| T9 再次部分退款 | 新 refund_id，重复 T6～T8，金额受累计可退额度约束 |

取消未支付订单单独 UPDATE order_header→CANCELLED 并写 cancelled_at，不插入成功支付流水。PAYMENT_FAILED 场景保留 CREATED 订单，表示支付失败后尚未超时取消，不能把失败直接等同于取消。

真实生产中：订单服务、支付服务、售后服务未必同库。T5 的单库事务是本地模拟实现，**不是声称微服务可以共享一个跨库本地事务**。跨服务应由支付服务可靠持久化成功事实，通过事务 Outbox/可靠事件和幂等消费更新订单服务，辅以补偿、渠道对账。本版不实现这些业务服务。

退款服务需要并发额度预占/锁定、失败释放、幂等渠道请求和回调、审计；模拟器单线程产生合法结果，不冒充可商用支付系统。Flink 不批准退款、不调用渠道、不修复业务数据库。

## 4. 第一层的维度数据与保存位置

主数据放在 MySQL 六张 `dim_*` 表，本例同库是为了联调；生产通常由商品中心、类目中心、品牌/商家中心、行政区划主数据分别维护。

公共字段：`id, name, parent_id, version, updated_at`。

| 维度表 | 附加字段/关系 | 本版分析口径 |
|---|---|---|
| dim_product | category_id、brand_id；parent_id=category_id | PRODUCT 指 SPU |
| dim_sku | product_id、unit_price_cent；parent_id=product_id | SKU 独立汇总 |
| dim_category | parent_id 指向上级类目或0 | 交易时的叶子类目，不自动递归汇总祖先 |
| dim_brand | parent_id=0 | 交易时品牌 |
| dim_shop | parent_id=0 | 子订单店铺 |
| dim_region | region_level=PROVINCE/CITY/DISTRICT，parent_id 构成层级 | 下单收货省、市、区快照，分别统计 |

模拟器初始化一套关联一致的维度：SKU→SPU→品牌/类目，订单行使用对应快照。不是对每个外键独立随机赋值。已有相同主键的维度不覆盖；若核心关联或价格不符合模拟字典则停止，避免写出不一致的模拟数据。结束时可配置更新一个店铺名称及版本，演示维度 CDC 更新。

金额归属采用**交易时 ID 快照**，展示名称通过 Doris 最新维度 LEFT JOIN（SCD Type 1）。维度晚到不丢交易金额，名称可暂用 ID。若要求历史名称、历史类目树，需 SCD Type 2/交易名称快照；本版未实现，不会自动拿今天的归属重算历史。

## 5. 第二层：Flink CDC → Kafka ODS

版本：Flink 1.19.3、Flink CDC 3.2.1。使用 Java `MySqlSource`，不是旧模块的 Kafka Connect，也不需要启动 Connect 服务。

CDC 首次做 initial snapshot，再持续读 binlog。从 checkpoint/savepoint 恢复时使用保存的快照进度和 binlog 位点，不是每次恢复都重新做全量。

Topic 全部从 `pipeline.properties` 读取：

| MySQL 表 | 默认 Kafka Topic |
|---|---|
| order_header | rttrade.ods.order_header |
| order_line | rttrade.ods.order_line |
| payment_attempt | rttrade.ods.payment_attempt |
| payment_ledger | rttrade.ods.payment_ledger |
| payment_line | rttrade.ods.payment_line |
| refund_header | rttrade.ods.refund_header |
| refund_line | rttrade.ods.refund_line |
| dim_product / dim_sku / dim_category / dim_brand / dim_shop / dim_region | rttrade.ods.<表名> |

Kafka Key=`after.id`（删除用 before.id），Value 保留 Flink CDC 的 Debezium JSON 信封，不只发 after。Flink 消费 Value 后提取 `order_id`，重新 keyBy 完成跨表关联。

```json
{
  "before": {"id":"O1-A1","order_id":"O1","status":"PENDING","version":1},
  "after": {"id":"O1-A1","order_id":"O1","status":"FAILED","version":2,
            "channel":"MOCK_PAY","amount_cent":21800,"failure_code":"INSUFFICIENT_FUNDS",
            "updated_at":1789207215000},
  "source": {"db":"rt_trade","table":"payment_attempt","file":"mysql-bin.000001","pos":1234},
  "op":"u",
  "ts_ms":1789207215100
}
```

这是裁剪示意，实际 CDC 包含更多 source 字段。`r`=快照、`c`=插入、`u`=更新、`d`=删除。快照只反映当前行，但订单保留 created_at、成功退款保留 succeeded_at，因此可恢复已完成的业务事实，而不要求 Kafka 中一定存在每个历史中间状态。

MySQL DATETIME(3) 使用 `time.precision.mode=connect` 输出毫秒逻辑值；解析器按无时区墙上时间还原，业务约定 Asia/Shanghai。不要把微秒值当毫秒，也不要把 ts_ms 当支付时间。切换 TIMESTAMP/UTC/多时区前必须同时修改并测试解析契约。本版事实只保留日期，规范化更新时间到秒，版本仍以 version 为准。

源表删除也进入 ODS，计算层将其隔离到 DLQ：金融事实不能在未定义冲正规则时自动删除。Unknown table/结构在 CDC 路由层失败而不是静默跳过，避免悄悄提交丢数据的位点。

## 6. 第三层：跨表关联 → DWD 交易商品事实

TradeFactJob：ODS 字段校验 → 按 order_id 分组 → 保存各表最新行 → 等待完整关联 → 生成不可变事实。

```text
订单头 + 完整订单行 ─────────────────────→ CREATED 商品行事实
取消订单头 + 完整订单行 ────────────────→ CANCELLED 商品行事实
成功支付流水 + 成功尝试 + 全部分摊 + 订单行 → PAID 商品行事实
成功退款头 + 全部退款行 + 已验证成功支付 ──→ REFUNDED 商品行事实
```

跨表的 binlog 同事务变化会逐条到达不同 Topic，Kafka 不保证跨 Topic 顺序；因此订单声明 line_count，资金分摊和退款分项也要齐备并通过金额校验，不能“来了第一行就当全部完成”。旧版本忽略，同版本相同内容去重，同版本不同内容报异常。候选状态校验通过后才提交。

Topic：**rttrade.dwd.trade_item_fact**。统一四种不可变事实，字段：

| 字段 | 类型/含义 |
|---|---|
| fact_id | CREATED:order_line.id / CANCELLED:order_line.id / PAID:payment_line.id / REFUNDED:refund_line.id |
| fact_type | CREATED、CANCELLED、PAID、REFUNDED |
| order_id / order_line_id | 交易订单与商品行 |
| business_id | CREATED/CANCELLED 为订单ID；PAID 为支付流水ID；REFUNDED 为退款单ID |
| user_id | 下单用户，统计支付人数用 |
| product_id / sku_id / category_id / brand_id / shop_id | 交易时维度 ID |
| province_id / city_id / district_id | 收货地区快照 |
| currency | CNY |
| event_date | 当前事件的业务日期；分别来自 created_at/cancelled_at/paid_at/succeeded_at |
| pay_date | PAID/REFUNDED 的原支付日；CREATED/CANCELLED 为 null |
| amount_cent | 本事件该商品行金额；退款为本次该行退款额 |
| quantity | CREATED/CANCELLED/PAID 为订单行商品件数；REFUNDED=0，不假装货物已退回 |

```json
{
  "fact_id":"REFUNDED:O1-R1-RL1","fact_type":"REFUNDED",
  "order_id":"O1","order_line_id":"O1-L1","business_id":"O1-R1","user_id":"U8",
  "product_id":"P1","sku_id":"SKU1","category_id":"C1","brand_id":"B1","shop_id":"S1",
  "province_id":"330000","city_id":"330100","district_id":"330106","currency":"CNY",
  "event_date":"2026-09-13","pay_date":"2026-09-12","amount_cent":3000,"quantity":0
}
```

## 7. 第四层：DWD → 实时指标

TradeMetricsJob 首先按 fact_id 去重；相同ID不同内容进入 DLQ，不覆盖 Doris 正确事实。随后展开 **ALL / PRODUCT / SKU / CATEGORY / BRAND / SHOP / PROVINCE / CITY / DISTRICT** 九种分组，按 `(biz_date, dimension_type, dimension_id)` 汇总。

内部 `MetricInput` 结构：

```json
{"bizDate":"2026-09-13","dimensionType":"PRODUCT","dimensionId":"P1",
 "phase":"EVENT","factJson":"这里是完整的DWD事实JSON字符串"}
```

REFUNDED 额外产生原支付日 `phase=NET_CORRECTION` 的输入；不是再扣一次当天退款额。其余事实只有 EVENT。

输出 Doris **rt_trade.ads_trade_metrics_day**，主键上述三个分组字段，币种本版固定 CNY：

| 指标列 | 计数/金额规则 | 日期 |
|---|---|---|
| created_order_count | 分组内 DISTINCT order_id，非商品行数 | 下单日 |
| cancelled_order_count | 分组内 DISTINCT order_id，仅未支付取消 | 取消日 |
| paid_order_count | 分组内 DISTINCT order_id，仅成功支付 | 支付日 |
| paid_user_count | 分组内 DISTINCT user_id | 支付日 |
| refund_order_count | 分组内 DISTINCT order_id，仅成功退款 | 退款成功日 |
| refund_request_count | 分组内 DISTINCT refund_header.id | 退款成功日 |
| paid_quantity | 成功支付商品件数，不因金额退款减件数 | 支付日 |
| created_amount_cent | 订单商品应付金额，含未付款订单，不含运费 | 下单日 |
| paid_amount_cent | 商品支付 GMV，不因退款减少 | 支付日 |
| refund_amount_cent | 成功商品退款金额 | 退款成功日 |
| net_paid_amount_cent | 原支付商品金额减之后成功退款 | 原支付日 |
| updated_at | 此次汇总输出时间；不是上游业务发生时间 | 处理时间 |

一张商品订单有两行，ALL 支付订单数仍为1；如果分别属于两个商品，两个商品分组各为1，不能加起来称“全站有2个支付订单”。支付人数也不能简单跨商品、跨店铺或跨日相加。金额在同一种维度类型内可加，**跨维度类型相加会重复计算**。

示例：12日支付100元、13日退30元。12日 paid=100、net=70、refund=0；13日 refund=30。跨日时同一行 net 不等于 paid-refund。

视图 **rt_trade.v_trade_dashboard** 额外给出：支付GMV/退款额/净GMV的元值、商品资金流净额（当日支付-当日退款）、平均支付订单金额（GMV/支付订单数）、人均支付金额（GMV/支付人数）。分母为0返回NULL，不伪装成0。未提供“当日支付订单数/当日下单订单数”的转化率，因为跨日支付会使这个比值失真；真正转化率需要同一批下单订单的 cohort 口径。

这里没有事件时间窗口关账。日键取业务日期，迟到事实可以更新历史日期。处理时间定时器只合并写出频率，并不决定业务日期。

## 8. 第五层：Doris 保存内容与维度流

- `dwd_trade_item_fact`：Unique Key(fact_id)，用于追溯和重建；pay_date 允许null。
- `ads_trade_metrics_day`：Unique Key(date,type,id)，Sink 写全量累计值，不是直接把 delta 再累加到数据库。
- `dim_catalog`：Unique Key(type,id)，version Sequence Column；name/parent_id 为查询字段，attributes_json 保存原维度行附加属性。
- `v_trade_dashboard`：LEFT JOIN 最新名称并计算展示指标；ALL 无维度行，使用 ALL 标记。

维度流：MySQL dim_* → Flink CDC → 对应 ODS Topic → DimensionSyncJob 解析、按主键检查版本 → Doris dim_catalog。改名只更新展示名称，不更改交易归属；老版本不能覆盖新版本。

## 9. 模拟器运行方式

配置入口 `pipeline.properties`，也可以给程序第一个参数传外部 properties 绝对路径。

- `mock.orders=90`：有限90个订单，9个场景循环10轮。
- `mock.orders.per.second=2`：目标上限2个订单/秒，不是2条CDC消息/秒；每订单含多个事务、每事务多行变化，实际受数据库写入和事务间隔限制。
- `mock.transaction.delay.ms=30`：业务事务之间的暂停，可调0；不在同一事务中途提交。
- `mock.run.id`：每次新生成订单必须换新前缀；重复执行同ID会触发主键冲突并停止，不覆盖历史订单，也不会自动续跑半完成订单。
- `mock.seed`：随机种子；数量、用户、店铺、地区、优惠可复现。
- `mock.base.time=AUTO`：默认用启动前10分钟作为业务基准，生命周期压缩发出；跨日退款订单创建时间回拨一天。显式历史基准格式 `2026-09-12 10:00:00`。不产生未来支付或退款时间。
- `mock.rename.dimension=true`：完成后改一次店铺名，演示维度更新。

九个场景：PAID、CANCELLED、PAYMENT_FAILED、RETRY_PAID、PARTIAL_REFUND、MULTIPLE_REFUNDS、FULL_GOODS_REFUND、REFUND_FAILED、CROSS_DAY_REFUND。

它是**业务逻辑真实、时间加速的有限模拟器**，不是实时等待24小时退款或真正向支付渠道发起交易。默认时间落在今日附近，不保证与电脑当前秒完全一致。每个完整9场景周期最终产生：18条创建商品事实、2条取消商品事实、14条支付商品事实、7条退款商品事实，共41条。完整90单共410条，金额随随机数量与优惠变化，以 `sql/reconcile.sql` 核对。按所有日期去重共有90单、取消10单、支付70单、成功退款40单/60个退款单；支付人数受用户随机重复影响，不能预写固定数。

## 10. 从空环境启动（操作顺序）

这些命令需要操作者主动执行；代码开发/单元测试不会自动向外部 Kafka、MySQL、Doris 写入。

### 10.1 准备 MySQL

使用 MySQL 8.0.x 开发环境（示例镜像8.0.40），ROW binlog、FULL row image，唯一 server-id。可在设置 RTTRADE_MYSQL_PASSWORD 后启动本目录的 `mysql-compose.yml`，也可让 DBA 在现有开发 MySQL 执行 `sql/mysql.sql`。端口3307若已被占用，修改 compose 和配置，**不要删除占用端口的旧数据库**。

```bash
docker compose -f src/main/resources/linyureal/realtime/mysql-compose.yml up -d
```

初始化 SQL 只在首次空数据卷执行。生产不要用删除数据卷的方式更新DDL；本版独立 rt_trade 库没有对旧 linyureal 表做迁移。数据库用户分离：模拟器写用户、CDC只读复制用户、Doris写用户。

由 DBA 创建 CDC 用户，开发示例（替换密码并限制来源）：

```sql
CREATE USER 'rt_cdc'@'%' IDENTIFIED BY 'REPLACE_WITH_STRONG_SECRET';
GRANT SELECT, SHOW VIEW ON rt_trade.* TO 'rt_cdc'@'%';
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'rt_cdc'@'%';
```

快照锁方式可能还需 LOCK TABLES 等权限，按实际 Flink CDC/MySQL 部署校验最小权限；生产须限制网段和TLS。binlog保留时间必须覆盖最大停机/快照时间。`cdc.server.id.range` 不能和MySQL服务器或其他CDC消费者冲突，范围数量要覆盖CDC并行度。

### 10.2 准备 Kafka 和 Doris

Kafka Topic 使用独立 rttrade 前缀。Doris 执行 `sql/doris.sql`。默认开发1副本，生产需要按实际节点设置副本、桶数和容量。

```bash
mvn test package
java -cp target/flink-datastream-job-all.jar org.linyureal.realtime.mock.CreateTopics /absolute/path/pipeline-local.properties
```

新建Topic默认3分区、1副本、delete策略、retention.ms=-1（只供实验完整重放）。生产要做保留期/归档/重建基线方案，不能无限保留又不监控磁盘。单节点Kafka若启用事务，需要配置事务状态日志副本数和minISR为1；生产保持高可用配置。broker transaction.max.timeout.ms 不小于配置600000ms。

### 10.3 提交四个 Flink 作业

Flink 1.19.3运行环境；本项目Java目标11。保证 RocksDB 后端依赖可用，上传 `target/flink-datastream-job-all.jar`。Web UI分别填写四个 Entry Class：

```text
org.linyureal.realtime.job.MysqlCdcToKafkaJob
org.linyureal.realtime.job.TradeFactJob
org.linyureal.realtime.job.TradeMetricsJob
org.linyureal.realtime.job.DimensionSyncJob
```

Program Arguments 填配置文件绝对路径，文件必须在执行main的集群环境存在，不能只是浏览器电脑上的路径。生产进程需要相应环境变量：CDC作业 RTTRADE_CDC_PASSWORD；指标/维度作业 RTTRADE_DORIS_PASSWORD。空密码也需要显式设为空字符串。模拟器运行环境设 RTTRADE_MYSQL_PASSWORD。配置与日志不要提交真实密码。

至少为4个作业准备足够slot。默认localhost地址只适用于相关程序和服务确实在同一网络环境；Flink在容器内时localhost指该容器，务必改成可解析的服务名或可达地址。Doris FE/BE Stream Load地址都必须能从TaskManager访问。

建议先启动 CDC 与下游，确认作业RUNNING，再启动模拟器；反过来先生成也可以，初始快照会补存量。

### 10.4 启动 MySQL 数据模拟

```bash
java -cp target/flink-datastream-job-all.jar org.linyureal.realtime.mock.MySqlSimulator /absolute/path/pipeline-local.properties
```

只写MySQL业务表/维度表，Kafka数据必须由 MysqlCdcToKafkaJob 真正读取数据库变化产生。检查 payment_attempt 中失败记录、payment_ledger 中只有成功记录，随后观察 ODS、DWD 和 Doris。

### 10.5 验证

执行 `sql/reconcile.sql`：源MySQL成功流水→Doris DWD→ADS 三层核对；等待输入消费追平、Checkpoint完成、Stream Load发布后再比。校验ALL组订单去重，商品/品类/店铺/品牌/省市区各维度金额守恒。

异常Topic=`rttrade.dlq`，值结构 `{"stage":"...","raw":"...","reason":"...","observed_at":"..."}`。原始输入及关联超时原因可追溯。DLQ是隔离通道，不是自动修复服务；生产必须接告警与人工核查/补偿。

## 11. 一致性、恢复和生产边界

1. CDC、DWD两级Kafka Sink均为exactly-once，下游read_committed；Doris启用2PC随checkpoint提交。多个作业和Doris多张表没有全局原子提交，不保证每个瞬间都相等。
2. 延迟会包含CDC checkpoint、事实Job checkpoint、指标输出合并间隔以及Doris提交，不能用3秒输出间隔宣称3秒端到端延迟。链末端Records Sent=0不等于未写外部存储。
3. checkpoint默认本地目录只用于本机实验；集群要换共享持久化存储，保留operator UID。已设置取消时保留checkpoint，Web UI重新提交时填完整的已完成checkpoint/savepoint路径，并验证状态兼容。
4. 新旧模块无状态兼容承诺。禁止拿旧作业的checkpoint恢复新模块；禁止两个独立实例同时写同一ADS累计表。不同环境配置不同Topic、consumer group、transaction prefix、Doris label prefix和库表。
5. 不配置资金状态/去重状态的自动TTL，避免长周期退款和历史重放导致重复金额。代价是订单状态、fact去重和精确人数去重持续增长；生产必须建立关闭周期、归档基线、补算与清理设计，压测后才可上线。ALL聚合也是容量瓶颈，需要按规模分层聚合；精确distinct不能直接相加。
6. Kafka历史过期或MySQL binlog被清理时不能静默latest跳过。缺全量基线时应受控重建到新表、对账后切换，不能把仅剩增量覆盖现有累计指标。
7. 不支持删除成功资金记录、修改已发布事实金额/维度ID或支付日期自动冲正。遇到冲突进入DLQ；需要更正时扩展有版本、可审计的正负冲正协议和重算流程。
8. 业务级异常被隔离后作业仍可RUNNING、checkpoint仍能成功；这不等于数据正确。需要DLQ、关联等待、source lag、checkpoint失败、Doris过滤/失败行、日对账差异、状态容量监控。
9. 本版未接支付渠道，没有实现业务服务鉴权、并发额度预占、事务Outbox、渠道日终对账、自动补偿、Kafka SASL/TLS配置和生产密钥托管。上线前必须补齐对应服务与运维能力，而非只看单元测试通过。

## 12. 测试与文件定位

```bash
mvn -Dtest='org.linyureal.realtime.*Test' test
mvn test package
```

测试不连接外部数据库：覆盖9场景、450组乱序重复、优惠金额/维度关联、订单人数distinct、跨日退款、缺失分摊等待、超额退款/同版本冲突、业务状态序列化重放、实际Flink CDC JSON序列化器及Kafka路由、本机Flink跨算子执行、指标定时器输出、维度版本更新。业务状态JSON往返不等于真实checkpoint故障恢复；外部MySQL→CDC→Kafka→Doris需要按步骤联调和故障演练。

- 配置：`src/main/resources/linyureal/realtime/pipeline.properties`
- 业务表/维度DDL：`src/main/resources/linyureal/realtime/sql/mysql.sql`
- Doris事实/汇总/维度/视图DDL：`src/main/resources/linyureal/realtime/sql/doris.sql`
- 对账：`src/main/resources/linyureal/realtime/sql/reconcile.sql`
- 模拟入口：`org.linyureal.realtime.mock.MySqlSimulator`

官方依据：[Flink CDC MySQL Source 3.2文档](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.2/docs/connectors/flink-sources/mysql-cdc/)、[Flink CDC版本兼容表](https://github.com/apache/flink-cdc#flink-version-compatibility)、[Flink CDC 3.2.1发布说明](https://flink.apache.org/2024/11/27/apache-flink-cdc-3.2.1-release-announcement/)。参考的是连接器用法与版本范围；具体业务指标口径由本模块显式定义。
