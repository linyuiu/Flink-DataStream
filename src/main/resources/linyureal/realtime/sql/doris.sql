CREATE DATABASE IF NOT EXISTS rt_trade;
-- One replica is for the local lab only. Configure production replication/buckets separately.
CREATE TABLE IF NOT EXISTS rt_trade.dwd_trade_item_fact (
 fact_id VARCHAR(100) NOT NULL, fact_type VARCHAR(16) NOT NULL,
 order_id VARCHAR(64) NOT NULL, order_line_id VARCHAR(64) NOT NULL, business_id VARCHAR(64) NOT NULL, user_id VARCHAR(64) NOT NULL,
 product_id VARCHAR(64) NOT NULL, sku_id VARCHAR(64) NOT NULL, category_id VARCHAR(64) NOT NULL,
 brand_id VARCHAR(64) NOT NULL, shop_id VARCHAR(64) NOT NULL,
 province_id VARCHAR(12) NOT NULL, city_id VARCHAR(12) NOT NULL, district_id VARCHAR(12) NOT NULL,
 currency CHAR(3) NOT NULL, event_date DATE NOT NULL, pay_date DATE NULL,
 amount_cent BIGINT NOT NULL, quantity BIGINT NOT NULL
) UNIQUE KEY(fact_id) DISTRIBUTED BY HASH(fact_id) BUCKETS 4
PROPERTIES("replication_num"="1","enable_unique_key_merge_on_write"="true");

CREATE TABLE IF NOT EXISTS rt_trade.ads_trade_metrics_day (
 biz_date DATE NOT NULL, dimension_type VARCHAR(16) NOT NULL, dimension_id VARCHAR(64) NOT NULL,
 currency CHAR(3) NOT NULL,
 created_order_count BIGINT NOT NULL, cancelled_order_count BIGINT NOT NULL,
 paid_order_count BIGINT NOT NULL, paid_user_count BIGINT NOT NULL,
 refund_order_count BIGINT NOT NULL, refund_request_count BIGINT NOT NULL, paid_quantity BIGINT NOT NULL,
 created_amount_cent BIGINT NOT NULL, paid_amount_cent BIGINT NOT NULL,
 refund_amount_cent BIGINT NOT NULL, net_paid_amount_cent BIGINT NOT NULL,
 updated_at DATETIME NOT NULL
) UNIQUE KEY(biz_date,dimension_type,dimension_id) DISTRIBUTED BY HASH(dimension_type,dimension_id) BUCKETS 4
PROPERTIES("replication_num"="1","enable_unique_key_merge_on_write"="true");

CREATE TABLE IF NOT EXISTS rt_trade.dim_catalog (
 dimension_type VARCHAR(16) NOT NULL, dimension_id VARCHAR(64) NOT NULL,
 name VARCHAR(200) NOT NULL, parent_id VARCHAR(64) NOT NULL,
 version BIGINT NOT NULL, attributes_json STRING NOT NULL
) UNIQUE KEY(dimension_type,dimension_id) DISTRIBUTED BY HASH(dimension_id) BUCKETS 4
PROPERTIES("replication_num"="1","enable_unique_key_merge_on_write"="true","function_column.sequence_col"="version");

CREATE VIEW IF NOT EXISTS rt_trade.v_trade_dashboard AS
SELECT m.*, COALESCE(d.name,m.dimension_id) AS dimension_name,
 CAST(m.paid_amount_cent/100.0 AS DECIMAL(20,2)) AS paid_gmv,
 CAST(m.refund_amount_cent/100.0 AS DECIMAL(20,2)) AS refund_amount,
 CAST(m.net_paid_amount_cent/100.0 AS DECIMAL(20,2)) AS net_paid_gmv,
 CAST((m.paid_amount_cent-m.refund_amount_cent)/100.0 AS DECIMAL(20,2)) AS goods_cash_net,
 CAST(m.paid_amount_cent/100.0/NULLIF(m.paid_order_count,0) AS DECIMAL(20,2)) AS avg_paid_order_amount,
 CAST(m.paid_amount_cent/100.0/NULLIF(m.paid_user_count,0) AS DECIMAL(20,2)) AS avg_paid_user_amount
FROM rt_trade.ads_trade_metrics_day m LEFT JOIN rt_trade.dim_catalog d
ON m.dimension_type=d.dimension_type AND m.dimension_id=d.dimension_id;
