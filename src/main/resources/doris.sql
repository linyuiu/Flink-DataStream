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