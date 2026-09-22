CREATE DATABASE IF NOT EXISTS commerce;
-- Development replication_num=1. Change both regular/dynamic replication policies before production creation.
CREATE TABLE IF NOT EXISTS commerce.metric_partial (
 biz_date DATE NOT NULL, dimension_type VARCHAR(16) NOT NULL, dimension_id VARCHAR(100) NOT NULL, shard_id INT NOT NULL,
 created_orders BIGINT NOT NULL, cancelled_orders BIGINT NOT NULL, paid_orders BIGINT NOT NULL,
 paid_users BIGINT NOT NULL, refund_orders BIGINT NOT NULL, refund_requests BIGINT NOT NULL, paid_quantity BIGINT NOT NULL,
 created_goods_cent BIGINT NOT NULL, paid_goods_cent BIGINT NOT NULL, refund_goods_cent BIGINT NOT NULL, net_paid_goods_cent BIGINT NOT NULL,
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
CREATE TABLE IF NOT EXISTS commerce.trade_event (
 event_date DATE NOT NULL, event_id VARCHAR(100) NOT NULL, event_type VARCHAR(24) NOT NULL,
 order_id VARCHAR(100) NOT NULL, payload_json STRING NOT NULL
) UNIQUE KEY(event_date,event_id)
PARTITION BY RANGE(event_date) ()
DISTRIBUTED BY HASH(event_id) BUCKETS 8
PROPERTIES(
 "replication_num"="1", "enable_unique_key_merge_on_write"="true",
 "dynamic_partition.enable"="true", "dynamic_partition.time_unit"="DAY", "dynamic_partition.time_zone"="Asia/Shanghai",
 "dynamic_partition.start"="-120", "dynamic_partition.end"="3", "dynamic_partition.prefix"="p",
 "dynamic_partition.buckets"="8", "dynamic_partition.replication_num"="1",
 "dynamic_partition.create_history_partition"="true", "dynamic_partition.history_partition_num"="120"
);
CREATE TABLE IF NOT EXISTS commerce.dim_catalog (
 dimension_type VARCHAR(16) NOT NULL, dimension_id VARCHAR(100) NOT NULL, name VARCHAR(200) NOT NULL,
 parent_id VARCHAR(100) NOT NULL, version BIGINT NOT NULL, attributes_json STRING NOT NULL
) UNIQUE KEY(dimension_type,dimension_id) DISTRIBUTED BY HASH(dimension_id) BUCKETS 4
PROPERTIES("replication_num"="1","enable_unique_key_merge_on_write"="true","function_column.sequence_col"="version");

CREATE VIEW IF NOT EXISTS commerce.metric_day AS
SELECT biz_date,dimension_type,dimension_id,
 SUM(created_orders) created_orders,SUM(cancelled_orders) cancelled_orders,SUM(paid_orders) paid_orders,
 SUM(paid_users) paid_users,SUM(refund_orders) refund_orders,SUM(refund_requests) refund_requests,SUM(paid_quantity) paid_quantity,
 SUM(created_goods_cent) created_goods_cent,SUM(paid_goods_cent) paid_goods_cent,SUM(refund_goods_cent) refund_goods_cent,
 SUM(net_paid_goods_cent) net_paid_goods_cent,MAX(updated_at_ms) updated_at_ms
FROM commerce.metric_partial GROUP BY biz_date,dimension_type,dimension_id;

CREATE VIEW IF NOT EXISTS commerce.dashboard AS
SELECT m.*,COALESCE(d.name,m.dimension_id) dimension_name,
 CAST(m.paid_goods_cent/100.0 AS DECIMAL(20,2)) paid_gmv,
 CAST(m.refund_goods_cent/100.0 AS DECIMAL(20,2)) refund_amount,
 CAST(m.net_paid_goods_cent/100.0 AS DECIMAL(20,2)) net_paid_gmv,
 CAST((m.paid_goods_cent-m.refund_goods_cent)/100.0 AS DECIMAL(20,2)) goods_cash_net,
 CAST(m.paid_goods_cent/100.0/NULLIF(m.paid_orders,0) AS DECIMAL(20,2)) paid_order_aov
FROM commerce.metric_day m LEFT JOIN commerce.dim_catalog d
ON m.dimension_type=d.dimension_type AND m.dimension_id=d.dimension_id;
