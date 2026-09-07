CREATE DATABASE IF NOT EXISTS linyureal;
-- Development replica count=1; production needs the actual replica policy.
CREATE TABLE IF NOT EXISTS linyureal.dwd_money_item_fact (
  fact_id VARCHAR(100) NOT NULL,
  fact_type VARCHAR(10) NOT NULL,
  order_id VARCHAR(80) NOT NULL,
  order_item_id VARCHAR(80) NOT NULL,
  product_id VARCHAR(80) NOT NULL, sku_id VARCHAR(80) NOT NULL,
  category_id VARCHAR(80) NOT NULL, brand_id VARCHAR(80) NOT NULL, shop_id VARCHAR(80) NOT NULL,
  province_code VARCHAR(12) NOT NULL, city_code VARCHAR(12) NOT NULL, district_code VARCHAR(12) NOT NULL,
  currency_code CHAR(3) NOT NULL, pay_date DATE NOT NULL, event_date DATE NOT NULL,
  amount_cent BIGINT NOT NULL
) UNIQUE KEY(fact_id)
DISTRIBUTED BY HASH(fact_id) BUCKETS 4
PROPERTIES("replication_num"="1","enable_unique_key_merge_on_write"="true");

CREATE TABLE IF NOT EXISTS linyureal.ads_gmv_dimension_day (
  biz_date DATE NOT NULL,
  dimension_type VARCHAR(16) NOT NULL,
  dimension_id VARCHAR(80) NOT NULL,
  currency_code CHAR(3) NOT NULL,
  paid_gmv DECIMAL(20,2) NOT NULL,
  refund_amount DECIMAL(20,2) NOT NULL,
  net_paid_gmv DECIMAL(20,2) NOT NULL,
  update_time DATETIME NOT NULL
) UNIQUE KEY(biz_date,dimension_type,dimension_id,currency_code)
DISTRIBUTED BY HASH(dimension_type,dimension_id) BUCKETS 4
PROPERTIES("replication_num"="1","enable_unique_key_merge_on_write"="true");

CREATE TABLE IF NOT EXISTS linyureal.dim_catalog (
  dimension_type VARCHAR(16) NOT NULL, dimension_id VARCHAR(80) NOT NULL,
  display_name VARCHAR(200) NOT NULL, parent_id VARCHAR(80) NOT NULL, version BIGINT NOT NULL
) UNIQUE KEY(dimension_type,dimension_id)
DISTRIBUTED BY HASH(dimension_id) BUCKETS 4
PROPERTIES("replication_num"="1","enable_unique_key_merge_on_write"="true","function_column.sequence_col"="version");
