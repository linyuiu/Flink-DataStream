-- Local development schema; deployment migrations must be controlled in production.
CREATE DATABASE IF NOT EXISTS linyureal CHARACTER SET utf8mb4;
USE linyureal;

CREATE TABLE IF NOT EXISTS trade_order (
  id VARCHAR(80) PRIMARY KEY,
  order_id VARCHAR(80) NOT NULL,
  version BIGINT NOT NULL CHECK (version > 0),
  update_time DATETIME(3) NOT NULL,
  user_id VARCHAR(80) NOT NULL, shop_id VARCHAR(80) NOT NULL,
  status VARCHAR(20) NOT NULL, currency_code CHAR(3) NOT NULL,
  province_code VARCHAR(12) NOT NULL, city_code VARCHAR(12) NOT NULL, district_code VARCHAR(12) NOT NULL,
  item_count INT NOT NULL CHECK(item_count > 0),
  goods_cent BIGINT NOT NULL CHECK(goods_cent > 0),
  freight_cent BIGINT NOT NULL CHECK(freight_cent >= 0),
  payable_cent BIGINT NOT NULL,
  UNIQUE KEY uq_order_id(order_id),
  CHECK(id = order_id), CHECK(payable_cent = goods_cent + freight_cent)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS trade_order_item (
  id VARCHAR(80) PRIMARY KEY,
  order_id VARCHAR(80) NOT NULL,
  version BIGINT NOT NULL CHECK (version > 0),
  update_time DATETIME(3) NOT NULL,
  sku_id VARCHAR(80) NOT NULL, product_id VARCHAR(80) NOT NULL,
  category_id VARCHAR(80) NOT NULL, brand_id VARCHAR(80) NOT NULL, shop_id VARCHAR(80) NOT NULL,
  quantity INT NOT NULL CHECK(quantity > 0), payable_cent BIGINT NOT NULL CHECK(payable_cent > 0),
  FOREIGN KEY(order_id) REFERENCES trade_order(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS trade_payment (
  id VARCHAR(80) PRIMARY KEY,
  order_id VARCHAR(80) NOT NULL,
  version BIGINT NOT NULL CHECK (version > 0),
  update_time DATETIME(3) NOT NULL,
  channel_transaction_id VARCHAR(100) NOT NULL UNIQUE,
  status VARCHAR(20) NOT NULL CHECK(status = 'SUCCEEDED'),
  amount_cent BIGINT NOT NULL CHECK(amount_cent > 0), pay_time DATETIME(3) NOT NULL,
  UNIQUE KEY uq_paid_order(order_id),
  FOREIGN KEY(order_id) REFERENCES trade_order(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS trade_payment_item (
  id VARCHAR(80) PRIMARY KEY,
  order_id VARCHAR(80) NOT NULL,
  version BIGINT NOT NULL CHECK (version > 0),
  update_time DATETIME(3) NOT NULL,
  payment_id VARCHAR(80) NOT NULL, order_item_id VARCHAR(80) NOT NULL UNIQUE,
  amount_cent BIGINT NOT NULL CHECK(amount_cent > 0),
  FOREIGN KEY(payment_id) REFERENCES trade_payment(id),
  FOREIGN KEY(order_item_id) REFERENCES trade_order_item(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS trade_refund (
  id VARCHAR(80) PRIMARY KEY,
  order_id VARCHAR(80) NOT NULL,
  version BIGINT NOT NULL CHECK (version > 0),
  update_time DATETIME(3) NOT NULL,
  payment_id VARCHAR(80) NOT NULL, status VARCHAR(20) NOT NULL,
  amount_cent BIGINT NOT NULL CHECK(amount_cent > 0), item_count INT NOT NULL CHECK(item_count > 0),
  refund_time DATETIME(3), FOREIGN KEY(payment_id) REFERENCES trade_payment(id),
  CHECK(status IN ('APPLIED','PROCESSING','SUCCEEDED','FAILED','CLOSED')),
  CHECK(status <> 'SUCCEEDED' OR refund_time IS NOT NULL)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS trade_refund_item (
  id VARCHAR(80) PRIMARY KEY,
  order_id VARCHAR(80) NOT NULL,
  version BIGINT NOT NULL CHECK (version > 0),
  update_time DATETIME(3) NOT NULL,
  refund_id VARCHAR(80) NOT NULL, order_item_id VARCHAR(80) NOT NULL,
  amount_cent BIGINT NOT NULL CHECK(amount_cent > 0),
  UNIQUE KEY uq_refund_item(refund_id,order_item_id),
  FOREIGN KEY(refund_id) REFERENCES trade_refund(id),
  FOREIGN KEY(order_item_id) REFERENCES trade_order_item(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_product (
  id VARCHAR(80) PRIMARY KEY, name VARCHAR(200) NOT NULL, parent_id VARCHAR(80) NOT NULL,
  version BIGINT NOT NULL, update_time DATETIME(3) NOT NULL
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS dim_sku (
  id VARCHAR(80) PRIMARY KEY, name VARCHAR(200) NOT NULL, parent_id VARCHAR(80) NOT NULL,
  version BIGINT NOT NULL, update_time DATETIME(3) NOT NULL
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS dim_category (
  id VARCHAR(80) PRIMARY KEY, name VARCHAR(200) NOT NULL, parent_id VARCHAR(80) NOT NULL,
  version BIGINT NOT NULL, update_time DATETIME(3) NOT NULL
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS dim_shop (
  id VARCHAR(80) PRIMARY KEY, name VARCHAR(200) NOT NULL, parent_id VARCHAR(80) NOT NULL,
  version BIGINT NOT NULL, update_time DATETIME(3) NOT NULL
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS dim_brand (
  id VARCHAR(80) PRIMARY KEY, name VARCHAR(200) NOT NULL, parent_id VARCHAR(80) NOT NULL,
  version BIGINT NOT NULL, update_time DATETIME(3) NOT NULL
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS dim_region (
  id VARCHAR(80) PRIMARY KEY, name VARCHAR(200) NOT NULL, parent_id VARCHAR(80) NOT NULL,
  version BIGINT NOT NULL, update_time DATETIME(3) NOT NULL, region_level VARCHAR(12) NOT NULL
) ENGINE=InnoDB;

INSERT IGNORE INTO dim_product VALUES ('P1','运动水杯','0',1,'2026-01-01'),('P2','棉质T恤','0',1,'2026-01-01');
INSERT IGNORE INTO dim_sku VALUES ('SKU1','蓝色水杯','P1',1,'2026-01-01'),('SKU2','白色T恤M码','P2',1,'2026-01-01');
INSERT IGNORE INTO dim_category VALUES ('C0','全部品类','0',1,'2026-01-01'),('C1','水杯','C0',1,'2026-01-01'),('C2','服装','C0',1,'2026-01-01');
INSERT IGNORE INTO dim_shop VALUES ('S1','杭州店','0',1,'2026-01-01'),('S2','上海店','0',1,'2026-01-01');
INSERT IGNORE INTO dim_brand VALUES ('B1','示例运动品牌','0',1,'2026-01-01'),('B2','示例服饰品牌','0',1,'2026-01-01');
INSERT IGNORE INTO dim_region VALUES
('310000','上海市','0',1,'2026-01-01','PROVINCE'),('310100','上海市','310000',1,'2026-01-01','CITY'),
('310115','浦东新区','310100',1,'2026-01-01','DISTRICT'),('330000','浙江省','0',1,'2026-01-01','PROVINCE'),
('330100','杭州市','330000',1,'2026-01-01','CITY'),('330106','西湖区','330100',1,'2026-01-01','DISTRICT');
