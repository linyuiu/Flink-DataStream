CREATE TABLE coupon_trigger_detail (
                                       trigger_id VARCHAR(128),
                                       user_id BIGINT,
                                       sku_id BIGINT,
                                       rule_code VARCHAR(64),
                                       trigger_time DATETIME,
                                       reason VARCHAR(255),
                                       dt DATE
)
    UNIQUE KEY(trigger_id)
DISTRIBUTED BY HASH(trigger_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1"
);

CREATE DATABASE IF NOT EXISTS linyu;

CREATE TABLE IF NOT EXISTS linyu.ads_realtime_gvm (
                                                      dt DATE,
                                                      stat_time DATETIME,
                                                      gmv DECIMAL(18,2),
    paid_order_count BIGINT,
    update_time DATETIME
    )
    DUPLICATE KEY(dt, stat_time)
    DISTRIBUTED BY HASH(dt) BUCKETS 1
    PROPERTIES (
                   "replication_num" = "1"
               );