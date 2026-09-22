-- 分拆指标表：先执行 doris.sql 创建公共审计/维度表，再执行本文件。
-- 金额与精确去重独立写入，报表查询 dashboard_v2。
-- 开发环境为单副本；生产同时修改普通与动态分区副本数。120 天动态分区会删除更早的数据。
CREATE TABLE IF NOT EXISTS commerce.metric_amount_partial (
 biz_date DATE NOT NULL, dimension_type VARCHAR(16) NOT NULL, dimension_id VARCHAR(100) NOT NULL, shard_id INT NOT NULL,
 created_orders BIGINT NOT NULL, cancelled_orders BIGINT NOT NULL, paid_orders BIGINT NOT NULL,
 refund_requests BIGINT NOT NULL, paid_quantity BIGINT NOT NULL, created_goods_cent BIGINT NOT NULL,
 paid_goods_cent BIGINT NOT NULL, refund_goods_cent BIGINT NOT NULL, net_paid_goods_cent BIGINT NOT NULL,
 update_seq BIGINT NOT NULL, updated_at_ms BIGINT NOT NULL
) UNIQUE KEY(biz_date,dimension_type,dimension_id,shard_id)
PARTITION BY RANGE(biz_date) ()
DISTRIBUTED BY HASH(dimension_type,dimension_id,shard_id) BUCKETS 8
PROPERTIES(
 "replication_num"="1", "enable_unique_key_merge_on_write"="true", "function_column.sequence_col"="update_seq",
 "dynamic_partition.enable"="true", "dynamic_partition.time_unit"="DAY", "dynamic_partition.time_zone"="Asia/Shanghai",
 "dynamic_partition.start"="-120", "dynamic_partition.end"="3", "dynamic_partition.prefix"="p",
 "dynamic_partition.buckets"="8", "dynamic_partition.replication_num"="1",
 "dynamic_partition.create_history_partition"="true", "dynamic_partition.history_partition_num"="120"
);

CREATE TABLE IF NOT EXISTS commerce.metric_distinct_partial (
 biz_date DATE NOT NULL, dimension_type VARCHAR(16) NOT NULL, dimension_id VARCHAR(100) NOT NULL, shard_id INT NOT NULL,
 paid_users BIGINT NOT NULL, refund_orders BIGINT NOT NULL,
 update_seq BIGINT NOT NULL, updated_at_ms BIGINT NOT NULL
) UNIQUE KEY(biz_date,dimension_type,dimension_id,shard_id)
PARTITION BY RANGE(biz_date) ()
DISTRIBUTED BY HASH(dimension_type,dimension_id,shard_id) BUCKETS 8
PROPERTIES(
 "replication_num"="1", "enable_unique_key_merge_on_write"="true", "function_column.sequence_col"="update_seq",
 "dynamic_partition.enable"="true", "dynamic_partition.time_unit"="DAY", "dynamic_partition.time_zone"="Asia/Shanghai",
 "dynamic_partition.start"="-120", "dynamic_partition.end"="3", "dynamic_partition.prefix"="p",
 "dynamic_partition.buckets"="8", "dynamic_partition.replication_num"="1",
 "dynamic_partition.create_history_partition"="true", "dynamic_partition.history_partition_num"="120"
);

-- 两个 Job 不能覆盖同一行中的不同字段：各写独立表，再合并查询。
-- 各表都是分片绝对值；只有取到每个主键的最新版本以后才能 SUM，不能相加历次更新。
CREATE VIEW IF NOT EXISTS commerce.metric_day_v2 AS
SELECT biz_date,dimension_type,dimension_id,
 SUM(created_orders) created_orders,SUM(cancelled_orders) cancelled_orders,SUM(paid_orders) paid_orders,
 SUM(paid_users) paid_users,SUM(refund_orders) refund_orders,SUM(refund_requests) refund_requests,
 SUM(paid_quantity) paid_quantity,SUM(created_goods_cent) created_goods_cent,
 SUM(paid_goods_cent) paid_goods_cent,SUM(refund_goods_cent) refund_goods_cent,SUM(net_paid_goods_cent) net_paid_goods_cent,
 MAX(updated_at_ms) updated_at_ms,MAX(amount_updated_at_ms) amount_updated_at_ms,MAX(distinct_updated_at_ms) distinct_updated_at_ms
FROM (
 SELECT biz_date,dimension_type,dimension_id,created_orders,cancelled_orders,paid_orders,
 CAST(0 AS BIGINT) paid_users,CAST(0 AS BIGINT) refund_orders,refund_requests,paid_quantity,
 created_goods_cent,paid_goods_cent,refund_goods_cent,net_paid_goods_cent,updated_at_ms,
 updated_at_ms amount_updated_at_ms,CAST(NULL AS BIGINT) distinct_updated_at_ms
 FROM commerce.metric_amount_partial
 UNION ALL
 SELECT biz_date,dimension_type,dimension_id,CAST(0 AS BIGINT),CAST(0 AS BIGINT),CAST(0 AS BIGINT),
 paid_users,refund_orders,CAST(0 AS BIGINT),CAST(0 AS BIGINT),CAST(0 AS BIGINT),CAST(0 AS BIGINT),CAST(0 AS BIGINT),CAST(0 AS BIGINT),
 updated_at_ms,CAST(NULL AS BIGINT),updated_at_ms
 FROM commerce.metric_distinct_partial
) partials
GROUP BY biz_date,dimension_type,dimension_id;

CREATE VIEW IF NOT EXISTS commerce.dashboard_v2 AS
SELECT m.*,COALESCE(d.name,m.dimension_id) dimension_name,
 CAST(m.paid_goods_cent/100.0 AS DECIMAL(20,2)) paid_gmv,
 CAST(m.refund_goods_cent/100.0 AS DECIMAL(20,2)) refund_amount,
 CAST(m.net_paid_goods_cent/100.0 AS DECIMAL(20,2)) net_paid_gmv,
 CAST((m.paid_goods_cent-m.refund_goods_cent)/100.0 AS DECIMAL(20,2)) goods_cash_net,
 CAST(m.paid_goods_cent/100.0/NULLIF(m.paid_orders,0) AS DECIMAL(20,2)) paid_order_aov
FROM commerce.metric_day_v2 m LEFT JOIN commerce.dim_catalog d
ON m.dimension_type=d.dimension_type AND m.dimension_id=d.dimension_id;
