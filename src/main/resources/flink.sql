CREATE TABLE coupon_trigger_kafka (
                                      trigger_id STRING,
                                      user_id BIGINT,
                                      sku_id BIGINT,
                                      rule_code STRING,
                                      trigger_time BIGINT,
                                      reason STRING,
                                      dt AS TO_DATE(FROM_UNIXTIME(trigger_time / 1000)),
                                      trigger_time_ltz AS TO_TIMESTAMP_LTZ(trigger_time, 3)
) WITH (
      'connector' = 'kafka',
      'topic' = 'coupon_trigger_topic',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'coupon-trigger-doris-sink',
      'scan.startup.mode' = 'latest-offset',
      'format' = 'json'
      );

CREATE TABLE coupon_trigger_doris (
                                      trigger_id STRING,
                                      user_id BIGINT,
                                      sku_id BIGINT,
                                      rule_code STRING,
                                      trigger_time TIMESTAMP(3),
                                      reason STRING,
                                      dt DATE
) WITH (
      'connector' = 'doris',
      'fenodes' = '127.0.0.1:8030',
      'table.identifier' = 'mall.coupon_trigger_detail',
      'username' = 'root',
      'password' = '',
      'sink.label-prefix' = 'coupon_trigger_sink'
      );

INSERT INTO coupon_trigger_doris
SELECT
    trigger_id,
    user_id,
    sku_id,
    rule_code,
    trigger_time_ltz,
    reason,
    dt
FROM coupon_trigger_kafka;
