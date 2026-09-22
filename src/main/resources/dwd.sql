set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_order_detail_inc partition (dt)
select
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_format(create_time, 'yyyy-MM-dd') date_id,
    create_time,
    source_id,
    source_type,
    dic_name,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount,
    date_format(create_time,'yyyy-MM-dd')
from
    (
        select
            data.id,
            data.order_id,
            data.sku_id,
            data.create_time,
            data.source_id,
            data.source_type,
            data.sku_num,
            data.sku_num * data.order_price split_original_amount,
            data.split_total_amount,
            data.split_activity_amount,
            data.split_coupon_amount
        from ods_order_detail_inc
        where dt = '2020-06-14'
          and type = 'bootstrap-insert'
    ) od
        left join
    (
        select
            data.id,
            data.user_id,
            data.province_id
        from ods_order_info_inc
        where dt = '2020-06-14'
          and type = 'bootstrap-insert'
    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            data.order_detail_id,
            data.activity_id,
            data.activity_rule_id
        from ods_order_detail_activity_inc
        where dt = '2020-06-14'
          and type = 'bootstrap-insert'
    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            data.order_detail_id,
            data.coupon_id
        from ods_order_detail_coupon_inc
        where dt = '2020-06-14'
          and type = 'bootstrap-insert'
    ) cou
    on od.id = cou.order_detail_id
        left join
    (
        select
            dic_code,
            dic_name
        from ods_base_dic_full
        where dt='2020-06-14'
          and parent_code='24'
    )dic
    on od.source_type=dic.dic_code;

load data inpath '/origin_data/$APP/db/${i:4}/$do_date' OVERWRITE into table ${APP}.$i partition(dt='$do_date');

DROP TABLE IF EXISTS dim_user_zip;
CREATE EXTERNAL TABLE dim_user_zip
(
    `id`           STRING COMMENT '用户id',
    `login_name`   STRING COMMENT '用户名称',
    `nick_name`    STRING COMMENT '用户昵称',
    `name`         STRING COMMENT '用户姓名',
    `phone_num`    STRING COMMENT '手机号码',
    `email`        STRING COMMENT '邮箱',
    `user_level`   STRING COMMENT '用户等级',
    `birthday`     STRING COMMENT '生日',
    `gender`       STRING COMMENT '性别',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `start_date`   STRING COMMENT '开始日期',
    `end_date`     STRING COMMENT '结束日期'
) COMMENT '用户表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_user_zip/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

insert overwrite table dim_user_zip partition (dt='9999-12-31')
select
    data.id,
    data.login_name,
    data.nick_name,
    md5(data.name),
    md5(if(data.phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',data.phone_num,null)),
    md5(if(data.email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',data.email,null)),
    data.user_level,
    data.birthday,
    data.gender,
    data.create_time,
    data.operate_time,
    '2020-06-14' start_date,
    '9999-12-31' end_date
from ods_user_info_inc
where dt='2020-06-14'
  and type='bootstrap-insert';


set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dim_user_zip partition(dt)
select
    id,
    login_name,
    nick_name,
    name,
    phone_num,
    email,
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    start_date,
    if(rn=2,date_sub('2020-06-15',1),end_date) end_date,
    if(rn=1,'9999-12-31',date_sub('2020-06-15',1)) dt
from
    (
        select
            id,
            login_name,
            nick_name,
            name,
            phone_num,
            email,
            user_level,
            birthday,
            gender,
            create_time,
            operate_time,
            start_date,
            end_date,
            row_number() over (partition by id order by start_date desc) rn
        from
            (
                select
                    id,
                    login_name,
                    nick_name,
                    name,
                    phone_num,
                    email,
                    user_level,
                    birthday,
                    gender,
                    create_time,
                    operate_time,
                    start_date,
                    end_date
                from dim_user_zip
                where dt='9999-12-31'
                union
                select
                    id,
                    login_name,
                    nick_name,
                    md5(name) name,
                    md5(if(phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',phone_num,null)) phone_num,
                    md5(if(email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',email,null)) email,
                    user_level,
                    birthday,
                    gender,
                    create_time,
                    operate_time,
                    '2020-06-15' start_date,
                    '9999-12-31' end_date
                from
                    (
                        select
                            data.id,
                            data.login_name,
                            data.nick_name,
                            data.name,
                            data.phone_num,
                            data.email,
                            data.user_level,
                            data.birthday,
                            data.gender,
                            data.create_time,
                            data.operate_time,
                            row_number() over (partition by data.id order by ts desc) rn
                        from ods_user_info_inc
                        where dt='2020-06-15'
                    )t1
                where rn=1
            )t2
    )t3;


DROP TABLE IF EXISTS ods_cart_info_full;
CREATE EXTERNAL TABLE ods_cart_info_full
(
    `id`           STRING COMMENT '编号',
    `user_id`      STRING COMMENT '用户id',
    `sku_id`       STRING COMMENT 'sku_id',
    `cart_price`   DECIMAL(16, 2) COMMENT '放入购物车时价格',
    `sku_num`      BIGINT COMMENT '数量',
    `img_url`      BIGINT COMMENT '商品图片地址',
    `sku_name`     STRING COMMENT 'sku名称 (冗余)',
    `is_checked`   STRING COMMENT '是否被选中',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间',
    `is_ordered`   STRING COMMENT '是否已经下单',
    `order_time`   STRING COMMENT '下单时间',
    `source_type`  STRING COMMENT '来源类型',
    `source_id`    STRING COMMENT '来源编号'
) COMMENT '购物车全量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_cart_info_full/';

DROP TABLE IF EXISTS dwd_trade_cart_full;
CREATE EXTERNAL TABLE dwd_trade_cart_full
(
    `id`       STRING COMMENT '编号',
    `user_id`  STRING COMMENT '用户id',
    `sku_id`   STRING COMMENT '商品id',
    `sku_name` STRING COMMENT '商品名称',
    `sku_num`  BIGINT COMMENT '加购物车件数'
) COMMENT '交易域购物车周期快照事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

insert overwrite table dwd_trade_cart_full partition(dt='2020-06-14')
select
    id,
    user_id,
    sku_id,
    sku_name,
    sku_num
from ods_cart_info_full
where dt='2020-06-14'
  and is_ordered='0';
