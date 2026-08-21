# 实时日 GMV：数据契约、外部依赖规范与代码改进建议

## 1. 文档范围

本文针对 `org.linyu.demo.RealtimeDailyGmvJob` 当前实现，约定上游 Kafka、
Doris、Flink 状态与发布流程应满足的条件，并记录代码侧后续可以实施的改进。

本次实际代码改动只涉及 `KafkaMockDataJob`。本文列出的 GMV 作业代码改进均为建议，
未修改 `RealtimeDailyGmvJob` 及其业务处理类。

## 2. 当前指标的准确含义

当前作业计算的不是“支付流水总额”，而是：

> 按原始支付日期归属的订单当前净支付 GMV。

单个订单当前贡献为：

```text
UNPAID / CANCELLED / CLOSED       -> 0
PAID                             -> pay_amount
PARTIALLY_REFUNDED / REFUNDED    -> pay_amount - refund_amount
```

退款会回溯修改原支付日期的 GMV。例如 8 月 1 日支付 100 元，8 月 3 日退款 30 元，
当前实现会把 8 月 1 日 GMV 从 100 元改成 70 元，而不是在 8 月 3 日记录 -30 元。

因此推荐将指标命名为 `net_paid_gmv_by_pay_date`，避免与以下指标混淆：

- `gross_paid_gmv`：只累计成功支付，不扣退款；
- `refund_amount_by_refund_date`：按退款发生日期统计退款；
- `net_cashflow`：按实际收付款发生日期统计净现金流。

业务看板通常应同时提供支付 GMV、退款金额和净支付 GMV，而不是只保留一个口径。

## 3. Kafka 订单快照契约

### 3.1 数据粒度

当前作业使用 `keyBy(order_id)` 保存一份订单贡献状态，因此输入必须满足：

- 一条消息代表一个订单的完整状态快照；
- 一个 `order_id` 对应一个逻辑订单聚合；
- 一个订单包含多个 SKU 时，由上游先汇总到订单级再写入该主题；
- 不允许把多条 SKU 明细使用同一个 `order_id` 直接输入当前作业，否则后一条明细会覆盖前一条贡献状态。

如果源系统只能提供订单明细粒度，则必须使用稳定的 `order_detail_id`，或使用
`order_id + sku_id` 作为状态主键，并重新定义退款如何分摊到明细。那属于另一种计算模型，
不能与当前订单级模型混用。

### 3.2 Kafka key 与消息类型

- Kafka key 必须为 `order_id`，同一个订单的所有版本必须进入同一分区；
- Kafka value 必须是全量快照，不是只包含变化字段的 patch；
- 主题建议命名为 `ods_order_snapshot` 或 `dwd_order_snapshot`，不建议继续使用
  `order_detail` 表示订单级数据；
- 主题名称必须由配置提供，不在 Java 代码中硬编码；
- `schema_version` 发生不兼容升级时，使用新版本或新主题，不允许静默改变字段语义。

当前项目为了保持 GMV 作业兼容，模拟器仍从配置项
`kafka.topic.gvm_realtime_produce` 读取主题。后续可以统一改名为
`kafka.topic.order.snapshot`，但生产者和消费者必须在同一次受控发布中切换。

### 3.3 字段规范

| 字段 | 类型 | 必填 | 约束与语义 |
| --- | --- | --- | --- |
| `schema_version` | INT | 是 | 当前为 `1` |
| `record_grain` | STRING | 是 | 固定为 `ORDER` |
| `event_id` | STRING | 是 | 每个快照事件唯一，用于追踪和幂等 |
| `order_id` | STRING | 是 | 订单业务主键，同时作为 Kafka key |
| `order_version` | BIGINT | 是 | 同一订单严格单调递增；不能只依赖秒级时间排序 |
| `user_id` | STRING | 是 | 下单用户标识 |
| `currency_code` | STRING | 是 | 当前作业只允许 `CNY` |
| `order_item_count` | INT | 是 | 订单商品项数量，必须大于 0 |
| `order_amount` | DECIMAL(18,2) | 是 | 订单应付总额，非当前 GMV 直接计算字段 |
| `pay_amount` | DECIMAL(18,2) | 是 | 原始成功支付总额；退款后仍保留原值 |
| `refund_amount` | DECIMAL(18,2) | 是 | 截至当前版本的累计退款总额，不是本次退款增量 |
| `order_status` | STRING | 是 | 使用本文定义的有限状态集合 |
| `create_time` | DATETIME | 是 | 订单创建时间 |
| `pay_time` | DATETIME/空 | 条件必填 | 支付成功后必填；退款版本仍保留原支付时间 |
| `refund_time` | DATETIME/空 | 条件必填 | 退款版本填写本次最新退款发生时间 |
| `update_time` | DATETIME | 是 | 快照生成/业务更新时间，主要用于审计 |
| `dt` | DATE | 是 | 快照更新日期；不是 GMV 的业务日期 |

金额统一使用两位小数的十进制定点数，不使用 `double` 执行业务计算。若未来支持多币种，
Doris 主键、Flink 聚合 key 和指标口径都必须增加 `currency_code`，不能直接把不同币种相加。

### 3.4 状态与金额不变量

| 状态 | `pay_amount` | `refund_amount` | `pay_time` | `refund_time` |
| --- | ---: | ---: | --- | --- |
| `UNPAID` | 0 | 0 | 空 | 空 |
| `CANCELLED` | 0 | 0 | 空 | 空 |
| `PAID` | `> 0` | 0 | 原支付时间 | 空 |
| `PARTIALLY_REFUNDED` | `> 0` | `> 0` 且 `< pay_amount` | 原支付时间 | 最新退款时间 |
| `REFUNDED` | `> 0` | `= pay_amount` | 原支付时间 | 最新退款时间 |

补充约束：

- 未支付订单可以进入 `CANCELLED`，已支付订单不能用 `CANCELLED` 表示退款；
- 已支付订单撤销资金必须使用退款状态和累计退款金额；
- `refund_amount` 只能单调增加，除非存在经过审计的退款冲正业务；
- 同一订单的 `pay_amount` 和 `pay_time` 在首次支付后保持不变；
- 消息重发时应保持原 `event_id` 和 `order_version`，不能生成一个伪造的新版本；
- 生产版本建议使用数据库 CDC 位点、事务序列或订单版本号作为 `order_version`。

### 3.5 生命周期示例

支付快照：

```json
{
  "schema_version": 1,
  "record_grain": "ORDER",
  "event_id": "O202608170001-v2",
  "order_id": "O202608170001",
  "order_version": 2,
  "currency_code": "CNY",
  "order_item_count": 2,
  "order_amount": 100.00,
  "pay_amount": 100.00,
  "refund_amount": 0.00,
  "order_status": "PAID",
  "create_time": "2026-08-17 09:00:00",
  "pay_time": "2026-08-17 09:01:00",
  "refund_time": "",
  "update_time": "2026-08-17 09:01:00",
  "dt": "2026-08-17"
}
```

之后累计退款 30 元的全量快照：

```json
{
  "schema_version": 1,
  "record_grain": "ORDER",
  "event_id": "O202608170001-v3",
  "order_id": "O202608170001",
  "order_version": 3,
  "currency_code": "CNY",
  "order_item_count": 2,
  "order_amount": 100.00,
  "pay_amount": 100.00,
  "refund_amount": 30.00,
  "order_status": "PARTIALLY_REFUNDED",
  "create_time": "2026-08-17 09:00:00",
  "pay_time": "2026-08-17 09:01:00",
  "refund_time": "2026-08-18 10:00:00",
  "update_time": "2026-08-18 10:00:00",
  "dt": "2026-08-18"
}
```

## 4. Doris 表规范

当前 Flink 作业每次输出的是某个 `biz_date` 的完整最新总额，不是要由 Doris 再次求和的增量。
目标表必须能够按业务日期覆盖更新，不能使用会重复累加的聚合模型。

参考 DDL：

```sql
CREATE TABLE realtime_ads.gmv_realtime_today_new (
    biz_date DATE NOT NULL COMMENT '原支付日期',
    gmv DECIMAL(18, 2) NOT NULL COMMENT '该支付日期当前净支付GMV',
    update_time DATETIME NOT NULL COMMENT 'Flink结果更新时间'
)
UNIQUE KEY (biz_date)
DISTRIBUTED BY HASH (biz_date) BUCKETS 1
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true"
);
```

生产规范：

- 表模型使用 Unique Key Merge-on-Write，主键至少包含 `biz_date`；
- 若扩展多币种或多指标，主键应改为 `biz_date + currency_code + metric_code`；
- `gmv` 使用 DECIMAL，精度根据最大日交易额设计；
- Flink 写入账号只授予目标库表所需权限，不使用 `root`；
- 监控 Doris Stream Load/事务失败、拒绝行数、写入耗时和表最后更新时间；
- 每日执行离线订单账与实时结果的对账，差异超过阈值告警并支持重算。

## 5. Flink 状态、Checkpoint 与发布规范

### 5.1 订单状态 TTL

订单贡献状态必须覆盖“最长允许退款/改单时间 + 最大数据延迟 + 故障恢复和回放余量”。
当前配置 `flink.state.order.ttl.days=7` 对常见退款周期通常过短。

错误示例：订单支付 100 元，状态 7 天后过期，随后累计退款快照为 30 元。
作业失去旧贡献 100 元，只会把当前净贡献 70 元当作新订单再次加上，结果可能从 100 变成 170。
全额退款在状态过期后甚至可能无法撤销旧贡献。

因此 TTL 不能凭经验固定为 7 天。若业务最长退款期为 90 天，可以从 120 天开始评估，
并通过状态大小、延迟分布和退款 SLA 再调整。超过在线状态保留期的数据必须进入补数/重算流程，
不能直接当作正常实时事件处理。

### 5.2 Checkpoint 存储

- 生产环境不得使用 TaskManager 本地 `/tmp` 作为 Checkpoint 的唯一存储；
- 使用所有 JobManager/TaskManager 都能访问的持久化存储，如 HDFS、S3/OSS 或持久化共享卷；
- 定期验证恢复，不以“Checkpoint 显示 Completed”代替恢复演练；
- 保留策略、对象存储生命周期和 Flink 外部化 Checkpoint 策略必须一致；
- 计划升级优先触发 Savepoint，再停止旧作业并从 Savepoint 恢复；
- 有状态算子的 `uid` 必须保持稳定，状态结构不兼容时制定迁移或全量重算方案。

### 5.3 Kafka 起点与发布模式

必须明确区分三种模式：

1. **正常恢复**：从已有 Checkpoint/Savepoint 恢复，Kafka offset 和算子状态一起恢复；
2. **从消费组继续**：无 Flink 状态时只使用 committed offset 会丢失订单历史贡献，不能用于当前有状态 GMV 作业的普通恢复；
3. **全量重算**：使用新的消费组和 `earliest`，写入新的 Doris 表或隔离的重算目标，完成对账后再切换。

不能把“换一个 group id 从 earliest 重放”直接写入现有结果表。Doris 中已有总额与 Flink 新建的空状态
会形成难以判断的一段过渡结果。

## 6. 密钥、配置和环境规范

- Kafka 地址、topic、group id、Doris FE、目标表和运行参数由环境配置提供；
- 用户名和密码从 Secret、环境变量或密钥管理服务注入，不提交到 Git；
- 当前 `bigdata.properties` 中存在明文 Doris/MySQL 密码，应迁移并立即轮换已暴露凭据；
- dev/test/prod 使用独立配置和独立 Kafka consumer group、Doris label prefix；
- 启动时打印非敏感配置摘要，并对缺失参数、空 topic、非法 TTL 和非法表名快速失败；
- 配置中的 topic 名不得带无意的前后空格。Java `Properties.load` 会忽略分隔符后的普通空白，
  但仍建议把当前 `kafka.topic.gvm_realtime_produce= ods_order_detail` 整理为无多余空格的统一格式，
  避免以后改用 YAML、环境变量或配置中心时产生不同解析结果。

## 7. 脏数据、监控和对账规范

脏数据不能只 `print` 到 TaskManager 日志。至少应写入独立 Kafka DLQ 或持久化表，字段包括：

- 原始消息、Kafka topic/partition/offset；
- `event_id`、`order_id`、`schema_version`；
- 失败阶段、错误码、错误原因和首次发现时间；
- 作业名、作业版本和重试状态。

建议监控：

- Kafka lag、输入速率和解析失败率；
- 业务校验失败率、旧版本丢弃数、未知状态数；
- GMV 正负变化量、退款比例和金额越界数；
- Checkpoint 时长、失败次数、状态大小和恢复耗时；
- Doris 写入成功/失败/拒绝行数及结果最后更新时间；
- 实时 GMV 与订单库离线汇总的日级差异。

## 8. GMV 代码侧改进建议（本次未修改）

### P0：正确性与可恢复性

1. **消费 `order_version`**：当前使用秒级 `update_time` 转毫秒作为版本，两个合法更新发生在同一秒时，后一条会被当作重复数据丢弃。优先使用严格单调的订单版本、CDC LSN 或事务序列。
2. **修复状态 TTL 风险**：由外部退款 SLA 推导 TTL，并为超期退款提供重算/冲正通道；只调大 TTL 仍不能代替长期账本。
3. **统一业务脏数据 OutputTag**：当前 `EnumV.BUSINESS_DIRTY_TAG` 与 `DirtyOutputTags.BUSINESS_DIRTY_TAG` 的 id 不一致，部分 `update_time` 格式错误可能写入未被主作业读取的侧输出。
4. **统一时间解析异常处理**：`pay_time` 的解析异常目前可能逃出业务校验并触发任务重启。时间格式校验应在状态读取/写入前完成，失败消息进入 DLQ，避免 poison message 反复重启。
5. **约束订单粒度**：解析或业务校验阶段验证 `record_grain=ORDER`、`currency_code=CNY` 和必填版本字段，不能只靠文档约定。
6. **明确 Doris 2PC 配置**：在代码或强类型配置中显式声明并验证写入语义、label prefix 和 Checkpoint 前置条件，避免依赖连接器默认值。

### P1：可维护性与可测试性

1. 将静态 `ConfigUtil` 调用集中到启动边界，构造不可变的 `RealtimeGmvConfig` 后注入 source、算子和 sink，便于单元测试及多环境覆盖。
2. 将 `EnumV` 拆为真正的业务枚举、时间格式常量和运行参数；当前类名与职责不符。
3. 输入模型、状态模型和 Doris 输出模型使用私有字段与明确访问方法，避免公开字段及字符串字段名造成跨层强耦合。
4. 使用 `DorisSinkConfig`/Builder 或工厂传参，替代密码、用户名、地址、表名、label prefix 等多个同类型位置参数。
5. 删除或统一未被主作业实际采用的重复配置加载器和 Sink 工厂，避免两套实现逐渐产生行为差异。
6. 类名、变量和注释统一 Java 规范；保留稳定 operator uid，不把显示 name 当作状态标识。
7. 增加指标定义类或枚举，明确输出的是 `net_paid_gmv_by_pay_date`，避免字段名 `gmv` 隐藏业务口径。

### P1：测试补充

现有业务测试覆盖支付、部分退款、全额退款、重复/旧版本和跨日期修正，这是良好基础。
还应增加：

- 同一秒两个不同 `order_version`；
- 多 SKU 输入被拒绝或上游已聚合；
- 订单状态 TTL 过期后的补偿策略；
- 非法 `pay_time`/`update_time` 不导致无限重启；
- 非法状态跳转和累计退款倒退；
- processing-time 定时输出；
- Checkpoint/Savepoint 恢复后不重复累计；
- Kafka 到 Doris 的 Testcontainers 集成测试；
- 离线订单账与 Doris 日 GMV 的对账测试。

## 9. 本次模拟器调整结果

`KafkaMockDataJob` 现在生成订单级全量快照，并保证：

- topic 继续从配置文件读取；
- 相同订单始终使用 `order_id` 作为 Kafka key；
- 移除具有误导性的随机 `sku_id`，改为订单级 `order_item_count`；
- 增加 `schema_version`、`record_grain`、`event_id`、`order_version` 和 `currency_code`；
- 部分退款使用统一状态 `PARTIALLY_REFUNDED`；
- `refund_amount` 始终表示累计退款金额；
- 退款快照保留原始 `pay_amount` 和 `pay_time`，并单独填写 `refund_time`；
- 每次状态变化递增 `order_version`，同一版本具有稳定的 `event_id`；
- producer 开启幂等并等待 broker 确认，发送失败会使模拟任务显式失败，而不是静默丢消息。

新增的契约字段当前会被 `OrderDetailRealTime` 通过 `ignoreUnknown=true` 安全忽略。
在 GMV 作业真正消费 `order_version` 之前，模拟器仍同时维护格式正确且递增的 `update_time`，
因此不会改变现有计算链路的行为。
