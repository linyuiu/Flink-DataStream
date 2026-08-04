# Flink 六周求职冲刺学习计划

> 适用对象：以前学过大数据开发，目前正在重新复习 Flink，并计划在 6 周内恢复到可以参加大数据开发面试的水平。  
> 学习主线：Flink / 大数据求职。  
> 辅助方向：Agent 开发作为副线，不占用核心学习时间。

---

## 一、学习资料应该怎么选

不要在“看文档”还是“看视频”之间二选一。

对于“以前学过，现在重新捡起来，并且目标是六周求职”的情况，最合适的学习方式是：

> **视频负责快速唤醒记忆，官方文档负责纠正和补全，代码实践负责真正掌握。**

推荐时间比例：

| 学习方式 | 建议占比 | 主要作用 |
|---|---:|---|
| 视频 | 20% | 建立直觉、快速回忆知识 |
| 官方文档 | 30% | 补充细节、确认准确语义 |
| 代码与排障 | 40% | 建立真正的实战能力 |
| 面试题复述 | 10% | 训练面试表达 |

不要连续看几个小时视频。

长时间看视频很容易产生“我好像都会了”的错觉，但真正写代码、解释原理和排查故障时，仍然可能说不清楚。

正确的学习闭环应该是：

> **问题驱动 → 视频建立直觉 → 文档确认细节 → 代码验证 → 面试表达**

---

## 二、学习版本选择

建议将学习主版本固定为：

> **Flink 1.20.x + Java + DataStream API + Flink SQL**

选择这个版本的原因：

1. 你之前已经使用过 Flink 1.19 和 1.20，重新上手成本较低。
2. Flink 1.20 属于长期支持版本，更适合当前企业项目和求职准备。
3. 六周时间有限，不适合在多个版本之间来回切换。
4. Java 仍然是国内大数据开发岗位最常见的 Flink 开发语言。

面试时可以这样表达：

> 我的项目主要使用 Flink 1.20，核心使用 DataStream API 和 Flink SQL，同时对 Flink 2.x 的主要变化有基本了解。

### 暂时不要分散精力

六周内不要同时做这些事情：

- 不要同时学习 Flink 1.17、1.19、1.20 和 2.x。
- 不要同时学习 Java、Scala 和 PyFlink。
- 不要一开始就深入所有源码细节。
- 不要同时跟三四套完整视频课程。
- 不要花大量时间比较不同培训机构的课程。

---

## 三、核心学习资料

## 3.1 Flink 1.20 官方文档

官方文档应该作为核心资料，但不需要从第一页一直看到最后一页。

重点阅读以下内容：

1. Flink 架构和核心概念
2. DataStream API
3. 时间语义与 Watermark
4. Window
5. State
6. Checkpoint、Savepoint 和故障恢复
7. Kafka Connector
8. Flink SQL
9. 状态后端
10. 部署、监控、背压与性能调优

官方中文文档适合快速理解。

遇到以下情况时，建议切换到英文文档核对：

- 配置参数
- API 方法签名
- 默认值
- 版本差异
- Exactly Once 语义
- Connector 兼容性

参考入口：

- Flink 官方文档：<https://nightlies.apache.org/flink/flink-docs-release-1.20/>
- Flink 1.20 中文文档：<https://nightlies.apache.org/flink/flink-docs-release-1.20/zh/>

---

## 3.2 官方 Fraud Detection 教程

官方 DataStream 入门教程会实现一个实时欺诈检测程序，适合作为第一周恢复手感的项目。

主要涉及：

- DataStream
- `keyBy`
- Keyed State
- Timer
- 告警输出
- 有状态流处理

它比 WordCount 更接近真实业务。

参考入口：

- <https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/try-flink/datastream/>

---

## 3.3 Apache Flink Training Course

Apache 官方提供了 Flink Training Course，通常包含：

- 理论课件
- 编程练习
- 示例项目
- 参考答案

它适合用来：

- 按章节补知识
- 做小型练习
- 检查自己是否真正理解
- 训练 DataStream API

参考入口：

- <https://flink.apache.org/what-is-flink/flink-training/>

注意：

> 不要把“看完课程”当作目标，最终目标是能写代码、能解释原理、能排查故障。

---

## 3.4 视频资料

视频只用于学习难以通过文字直接建立直觉的内容。

适合看视频的专题：

- Checkpoint 执行流程
- Barrier 对齐
- Exactly Once
- 状态后端
- 背压传播
- JobManager 和 TaskManager
- Slot 与并行度
- 作业提交与调度流程
- Flink SQL 动态表
- Kafka 与 Flink 的协作机制

推荐优先搜索：

- Flink Forward 官方演讲
- Apache Flink 官方频道
- Flink Checkpoint
- Flink Barrier Alignment
- Flink State Backend
- Flink Backpressure
- Flink ExecutionGraph

不要从一套课程第一集开始连续往后看。

更高效的方式是：

> 今天学习 Barrier 对齐，就只看 Barrier 对齐相关视频。

---

## 四、六周学习安排

# 第 1 周：恢复基础与编程手感

## 学习目标

重新建立对 Flink 基础架构、DataStream API 和程序运行方式的理解。

## 学习内容

- Flink 的应用场景
- 批处理与流处理
- 有界流与无界流
- JobManager
- TaskManager
- Client
- ResourceManager
- Dispatcher
- Slot
- 并行度
- Source
- Transformation
- Sink
- `map`
- `flatMap`
- `filter`
- `keyBy`
- `reduce`
- `process`
- Operator Chain

## 实践任务

完成以下程序：

1. WordCount
2. Kafka → Flink → 控制台
3. 订单金额实时聚合
4. 官方 Fraud Detection 教程
5. 自定义 Source 或测试数据源
6. 使用 Web UI 查看作业拓扑

## 本周验收标准

你应该能够脱离视频，独立写出一个基本的 DataStream 程序。

你还应该能够说明：

- JobManager 和 TaskManager 分别负责什么
- Slot 和并行度有什么关系
- `keyBy` 为什么会触发数据重分区
- Operator Chain 有什么作用
- Flink 作业从提交到运行的大致过程

---

# 第 2 周：时间、Watermark 与窗口

## 学习目标

掌握乱序数据、事件时间和窗口计算，这是 Flink 面试中的高频内容。

## 学习内容

- Event Time
- Processing Time
- Ingestion Time 的历史概念
- Timestamp Assigner
- Watermark
- Watermark 生成策略
- Watermark 传播
- 乱序数据
- 空闲分区
- 滚动窗口
- 滑动窗口
- 会话窗口
- 全局窗口
- Window Assigner
- Trigger
- Evictor
- Window Function
- ProcessWindowFunction
- 增量聚合
- 全量聚合
- Allowed Lateness
- Side Output
- 窗口 Join
- Interval Join

## 实践项目

实现一个实时订单 GMV 任务：

> Kafka 订单流 → Flink 分钟级窗口聚合 → 处理迟到订单 → 输出到 Doris

项目至少包含：

- 订单事件时间字段
- Watermark
- 滚动窗口
- 允许迟到时间
- 侧输出流
- 按商品或地区分组
- 输出实时 GMV

## 本周验收标准

你应该能够回答：

- Event Time 和 Processing Time 有什么区别
- Watermark 是什么
- Watermark 为什么不是一条真实业务数据
- Watermark 如何在并行算子之间传播
- 某个分区长时间没有数据会有什么问题
- Allowed Lateness 和 Side Output 有什么区别
- 滚动窗口和滑动窗口有什么区别
- 窗口什么时候触发
- 延迟数据如何处理

---

# 第 3 周：State、Checkpoint 与 Exactly Once

## 学习目标

这是六周中最重要的一周，也是 Flink 面试的核心。

## 学习内容

### State

- Keyed State
- Operator State
- ValueState
- ListState
- MapState
- ReducingState
- AggregatingState
- Broadcast State
- State TTL
- 状态生命周期
- 状态序列化

### Checkpoint

- Checkpoint 的作用
- CheckpointCoordinator
- Barrier 的产生
- Barrier 的传播
- Barrier 对齐
- 对齐期间的数据处理
- Checkpoint 超时
- Checkpoint 并发数
- Min Pause
- Unaligned Checkpoint
- Incremental Checkpoint
- Checkpoint 清理策略

### 状态后端

- HashMapStateBackend
- EmbeddedRocksDBStateBackend
- JobManagerCheckpointStorage
- FileSystemCheckpointStorage
- 本地状态与远程状态
- RocksDB 增量 Checkpoint

### 故障恢复

- Restart Strategy
- Fixed Delay
- Failure Rate
- Exponential Delay
- Task 故障恢复
- Region Failover
- 从 Checkpoint 恢复
- 从 Savepoint 恢复

### Exactly Once

- At Most Once
- At Least Once
- Exactly Once
- Source 位点
- Flink State
- Sink 事务
- 两阶段提交
- 幂等写入
- Kafka Exactly Once
- Doris Sink 事务语义

## 实践任务

1. 写一个使用 `ValueState` 的订单去重程序。
2. 开启 Checkpoint。
3. 设置 Checkpoint 存储目录。
4. 人为杀死 TaskManager。
5. 观察作业自动恢复。
6. 在 Web UI 查看 Checkpoint History。
7. 制造背压。
8. 观察 Checkpoint Duration。
9. 查看 Alignment Duration。
10. 创建 Savepoint。
11. 停止作业。
12. 从 Savepoint 恢复。
13. 测试修改算子 UID 后能否恢复。
14. 排查 Doris Sink 事务或 Label 问题。

## 本周验收标准

你应该能够画出完整的 Checkpoint 流程，并回答：

- Barrier 从哪里产生
- Barrier 如何进入 Source
- 为什么需要 Barrier 对齐
- 对齐期间后到达的数据如何处理
- 背压为什么会影响 Checkpoint
- Unaligned Checkpoint 解决了什么问题
- Checkpoint 和 Savepoint 有什么区别
- Exactly Once 是否代表外部系统一定不会出现重复数据
- Flink 如何保证 Kafka Source 的一致性
- Sink 如何实现 Exactly Once
- 两阶段提交有什么缺点

---

# 第 4 周：Kafka、Flink SQL、Doris 与 CDC

## 学习目标

建立一套完整的实时数仓技术链路。

## Kafka 学习内容

- Topic
- Partition
- Replica
- Leader
- Follower
- ISR
- Consumer Group
- Offset
- Rebalance
- Partition 分配策略
- Kafka 消费积压
- KafkaSource
- KafkaSink
- 起始位点
- Offset 提交
- Kafka 事务
- Flink 并行度与 Kafka Partition 的关系

## Flink SQL 学习内容

- Dynamic Table
- Changelog
- Append Stream
- Update Stream
- Retract Stream
- Upsert
- Table API
- SQL Client
- Connector Table
- Computed Column
- Metadata Column
- Watermark DDL
- Group Aggregation
- Window TVF
- TUMBLE
- HOP
- CUMULATE
- Window Join
- Interval Join
- Temporal Join
- Lookup Join
- Top-N
- Deduplication

## CDC 学习内容

- Flink CDC 工作原理
- MySQL Binlog
- Snapshot
- 增量读取
- 全量阶段与增量阶段
- Schema Change
- Exactly Once
- CDC 表同步
- 主键与更新流

## Doris 学习内容

- Stream Load
- Routine Load
- Flink Doris Connector
- Label
- Transaction
- 两阶段提交
- Unique Key 模型
- Duplicate Key 模型
- Aggregate Key 模型
- Primary Key 模型
- 数据写入失败排查

## 实践项目

构建一个实时广告或电商数仓：

> MySQL CDC → Kafka / Flink → DWD → DWS → Doris

建议业务指标：

- 实时订单量
- 实时 GMV
- 支付转化率
- 广告曝光量
- 广告点击量
- 点击率
- 用户活跃数
- 商品销售 Top-N
- 地区销售排名
- 未支付订单超时统计

## 本周验收标准

你应该能够说明：

- Kafka Partition 数量与 Flink Source 并行度的关系
- Kafka Rebalance 会带来什么问题
- Flink SQL 动态表是什么
- Changelog 为什么会有 `+I`、`-U`、`+U`、`-D`
- Lookup Join 适合什么场景
- CDC 全量阶段与增量阶段如何衔接
- Doris Unique Key 模型如何处理更新
- Doris Sink 的 Label 有什么作用

---

# 第 5 周：部署、监控、性能与生产排障

## 学习目标

从“会写代码”提升到“能维护生产任务”。

## 部署模式

- Standalone
- Yarn Session
- Yarn Per-Job
- Yarn Application Mode
- Kubernetes Session
- Kubernetes Application Mode
- Native Kubernetes
- Docker Compose 测试环境

## Web UI

重点掌握：

- Jobs
- Running Jobs
- Completed Jobs
- Exceptions
- Checkpoints
- Backpressure
- Task Metrics
- Watermarks
- Records In
- Records Out
- Bytes In
- Bytes Out
- Busy Time
- Idle Time
- Backpressured Time

## 常见故障

- Kafka 消费积压
- Checkpoint 超时
- Checkpoint 失败
- Checkpoint 体积过大
- 反压
- 数据倾斜
- TaskManager OOM
- JobManager OOM
- Slot 不足
- 网络缓冲区不足
- RocksDB 写入慢
- 状态过大
- Full GC
- 序列化失败
- 类冲突
- Connector 版本不兼容
- 数据重复
- 数据丢失
- Watermark 不推进
- 窗口不触发
- Savepoint 恢复失败
- 算子 UID 不兼容
- Doris 写入失败
- Kafka Topic 不存在

## 排障方法

每个问题都按照以下模板记录：

```text
1. 故障现象
2. 影响范围
3. Web UI 指标
4. 日志关键报错
5. 初步假设
6. 验证方法
7. 根因
8. 解决方案
9. 修复结果
10. 如何预防
```

## 本周验收标准

至少整理 10 个生产故障案例。

面试时不要只说：

> 我会看日志。

而应该能够具体说明：

> 我先看 Kafka Lag，再看 Flink Web UI 的 Records In、Records Out 和 Backpressured Time。如果 Source 读取正常但下游 Records Out 明显下降，我会继续定位具体算子的反压情况，并检查是否存在外部 Sink 写入变慢、数据倾斜或状态访问延迟。

---

# 第 6 周：源码主流程、项目包装与模拟面试

## 学习目标

完成求职前最后的知识整合。

## 源码主流程

只学习主链路，不要陷入所有源码细节。

重点了解：

- 作业提交入口
- StreamGraph
- JobGraph
- ExecutionGraph
- ExecutionJobVertex
- ExecutionVertex
- Execution
- Operator Chain
- Task
- StreamTask
- SourceTask
- OneInputStreamTask
- TwoInputStreamTask
- CheckpointCoordinator
- BarrierHandler
- State 初始化
- State 恢复
- 调度与 Slot 分配

## 项目包装

至少准备一套完整项目：

### 项目示例

**实时广告或电商数据仓库**

技术栈：

- MySQL
- Flink CDC
- Kafka
- Flink DataStream
- Flink SQL
- Doris
- Redis
- Prometheus
- Grafana

项目内容：

- 数据采集
- 数据清洗
- 维度关联
- 实时聚合
- 指标计算
- 数据写入
- 延迟处理
- 状态管理
- Checkpoint
- 故障恢复
- 监控告警
- 性能优化

## 项目介绍结构

面试时按照以下结构回答：

1. 项目背景
2. 业务目标
3. 技术架构
4. 自己负责的模块
5. 核心指标
6. 数据规模
7. 技术难点
8. 故障案例
9. 性能优化
10. 最终效果

## 面试冲刺任务

- 30 道 Flink 高频面试题
- 20 道 Kafka 高频面试题
- 10 道实时数仓设计题
- 10 个生产故障案例
- 3 次模拟面试
- 1 份完整项目介绍
- 1 张实时数仓架构图
- 1 份简历项目描述
- 1 份自我介绍

---

## 五、每天怎么学习

建议工作日安排两个 45 分钟学习块。

# 第一个 45 分钟：理解原理

建议安排：

- 15 分钟：观看一个专题视频
- 25 分钟：阅读对应官方文档
- 5 分钟：合上资料，用自己的话复述

例如当天主题是 Barrier 对齐：

1. 看 Barrier 对齐动画或视频。
2. 阅读官方 Checkpoint 文档。
3. 不看资料，回答以下问题：
   - Barrier 从哪里产生？
   - 为什么需要对齐？
   - 对齐期间数据去了哪里？
   - 为什么背压会导致 Checkpoint 变慢？
   - Unaligned Checkpoint 解决了什么问题？

# 第二个 45 分钟：代码与实践

可以选择以下任务：

- 写一个最小示例
- 修改一个配置
- 人为制造故障
- 查看 Web UI
- 阅读错误日志
- 从 Savepoint 恢复
- 测试迟到数据
- 测试 Kafka 积压
- 调整并行度
- 查看 Checkpoint 指标

每天必须形成一个可见产出：

- 一个 Git Commit
- 一页 Markdown 笔记
- 一张流程图
- 一个排障记录
- 五道面试题答案
- 一个可运行 Demo

---

## 六、每周时间安排建议

| 日期 | 主任务 |
|---|---|
| 周一 | 学习新知识点 |
| 周二 | 学习新知识点并写 Demo |
| 周三 | 学习新知识点并做实验 |
| 周四 | 故障模拟与排查 |
| 周五 | 面试题复述与本周总结 |
| 周六 | 完整项目实践 |
| 周日 | 复盘、简历、模拟面试、少量 Agent 学习 |

建议时间分配：

- Flink / 大数据主线：80%
- Agent 开发副线：20%

Agent 学习可以安排在周六晚上或周日下午，不要每天在两个方向之间切换。

---

## 七、视频和文档的正确使用方式

## 错误方式

```text
从第一集开始连续看课程。
每天看两三个小时视频。
准备全部看完之后再写代码。
看懂了就认为自己掌握了。
```

这种方式容易导致：

- 注意力下降
- 知识遗忘
- 缺少代码能力
- 面试无法表达
- 产生虚假的学习成就感

## 正确方式

```text
今天只学习 Watermark。
先看 15 分钟视频建立直觉。
再看官方文档确认语义。
然后写一个乱序数据 Demo。
最后回答 5 道面试题。
```

每个知识点都应该形成闭环：

> **理解 → 验证 → 输出 → 复述**

---

## 八、学习时如何减少分心

由于 X 和抖音容易打断学习，建议配合以下规则。

### 学习环境

- 手机不放在书桌上。
- 学习期间关闭所有通知。
- 抖音和 X 尽量从手机卸载。
- 必须使用时，只在电脑网页登录。
- 不保存登录密码。
- 使用系统应用限额。
- 学习开始前关闭无关网页。

### 学习规则

- 短视频只能在完成学习后观看。
- 不允许在学习开始前刷短视频。
- 每次只学习一个主题。
- 想查其他内容时，先记在纸上，不立即切换。
- 状态不好时，至少完成 25 分钟。

### 每日记录

每天只记录三个指标：

| 日期 | 深度学习时间 | 今日产出 | 短视频时间 |
|---|---:|---|---:|
| 示例 | 90 分钟 | 完成 Checkpoint 笔记 | 30 分钟 |

---

## 九、六周学习的最终目标

六周后，你不应该只是“看完了一套课程”。

你应该能够做到：

1. 给出一个实时业务需求，能够设计完整架构。
2. 能够独立写出 DataStream 程序。
3. 能够使用 Flink SQL 完成实时指标计算。
4. 能够解释 Watermark、Window、State 和 Checkpoint。
5. 能够解释 Flink Exactly Once。
6. 能够排查 Kafka 积压、反压和 Checkpoint 超时。
7. 能够使用 Savepoint 完成作业升级。
8. 能够解释 Flink 与 Kafka、Doris、CDC 的关系。
9. 能够讲清楚一个完整的实时数仓项目。
10. 能够回答 Flink 高频面试题。

最终标准是：

> **给你一个实时需求，你能设计架构、写出代码、解释运行原理，并排查任务故障。**

---

## 十、今天开始执行

今天先完成以下任务：

1. 确定主版本为 Flink 1.20.x。
2. 整理本地 Kafka、Flink 和 Doris 环境。
3. 阅读 Flink 架构与 DataStream 基础。
4. 完成官方 Fraud Detection 教程。
5. 写一份笔记：
   - JobManager
   - TaskManager
   - Slot
   - 并行度
   - Operator Chain
6. 提交一次 Git Commit。
7. 学习结束后，再使用抖音或 X。

不要继续寻找更多课程。

从现在开始，按照固定资料和固定节奏执行六周。
