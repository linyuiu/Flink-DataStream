# Flink-DataStream

这是一个基于 Apache Flink DataStream API 的实时营销规则项目。

项目从 Kafka 读取购物车加购 CDC 数据和订单支付 CDC 数据，解析成业务事件后，按“用户 + SKU”维度判断营销候选规则，再按用户维度做发券频控，最终把可触达的优惠券触发事件写回 Kafka。下游可以通过 Flink SQL 将触发事件同步到 Doris。

## 实时链路

1. `ods_cart_add_event`：购物车加购事件，来自 Debezium CDC。
2. `ods_order_item`：订单明细/支付事件，来自 Debezium CDC。
3. `UserSkuRuleFunction`：识别“加购 30 分钟未支付”和“1 小时内多次加购同一 SKU”等候选规则。
4. `UserFrequencyControlFunction`：按用户做触达频控，避免同规则、同商品、同日或疑似薅券用户被频繁触达。
5. `coupon_trigger_topic`：输出最终优惠券触发事件。
6. `flink.sql` + `doris.sql`：示例下游表和同步语句，用于把触发明细落到 Doris。

## 核心入口

主作业入口是：

```text
src/main/java/org/linyu/marketing/RealtimeMarketingJob.java
```

运行前需要准备本地 Kafka、对应 topic，以及 Flink checkpoint 存储目录。当前代码中的 broker、topic 和 Doris 地址都是本地开发配置。
