-- Dedicated new schema; never recreates existing linyureal tables.
CREATE DATABASE IF NOT EXISTS rt_trade CHARACTER SET utf8mb4;
USE rt_trade;

CREATE TABLE IF NOT EXISTS dim_category (
 id VARCHAR(64) PRIMARY KEY, name VARCHAR(200) NOT NULL, parent_id VARCHAR(64) NOT NULL,
 version BIGINT NOT NULL CHECK(version > 0), updated_at DATETIME(3) NOT NULL
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS dim_brand LIKE dim_category;
CREATE TABLE IF NOT EXISTS dim_shop LIKE dim_category;
CREATE TABLE IF NOT EXISTS dim_product (
 id VARCHAR(64) PRIMARY KEY, name VARCHAR(200) NOT NULL, parent_id VARCHAR(64) NOT NULL,
 version BIGINT NOT NULL CHECK(version > 0), updated_at DATETIME(3) NOT NULL,
 category_id VARCHAR(64) NOT NULL, brand_id VARCHAR(64) NOT NULL,
 FOREIGN KEY(category_id) REFERENCES dim_category(id), FOREIGN KEY(brand_id) REFERENCES dim_brand(id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS dim_sku (
 id VARCHAR(64) PRIMARY KEY, name VARCHAR(200) NOT NULL, parent_id VARCHAR(64) NOT NULL,
 version BIGINT NOT NULL CHECK(version > 0), updated_at DATETIME(3) NOT NULL,
 product_id VARCHAR(64) NOT NULL, unit_price_cent BIGINT NOT NULL CHECK(unit_price_cent > 0),
 FOREIGN KEY(product_id) REFERENCES dim_product(id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS dim_region (
 id VARCHAR(64) PRIMARY KEY, name VARCHAR(200) NOT NULL, parent_id VARCHAR(64) NOT NULL,
 version BIGINT NOT NULL CHECK(version > 0), updated_at DATETIME(3) NOT NULL,
 region_level VARCHAR(12) NOT NULL CHECK(region_level IN ('PROVINCE','CITY','DISTRICT'))
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS order_header (
 id VARCHAR(64) PRIMARY KEY, order_id VARCHAR(64) NOT NULL UNIQUE,
 version BIGINT NOT NULL CHECK(version > 0), updated_at DATETIME(3) NOT NULL,
 user_id VARCHAR(64) NOT NULL, shop_id VARCHAR(64) NOT NULL,
 province_id VARCHAR(12) NOT NULL, city_id VARCHAR(12) NOT NULL, district_id VARCHAR(12) NOT NULL,
 currency CHAR(3) NOT NULL CHECK(currency='CNY'),
 status VARCHAR(16) NOT NULL CHECK(status IN ('CREATED','PAID','CANCELLED','COMPLETED')),
 created_at DATETIME(3) NOT NULL, cancelled_at DATETIME(3),
 line_count INT NOT NULL CHECK(line_count > 0), goods_cent BIGINT NOT NULL CHECK(goods_cent > 0),
 freight_cent BIGINT NOT NULL CHECK(freight_cent >= 0), payable_cent BIGINT NOT NULL,
 CHECK(id=order_id), CHECK(payable_cent=goods_cent+freight_cent), CHECK(status<>'CANCELLED' OR cancelled_at IS NOT NULL),
 KEY idx_created(created_at)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS order_line (
 id VARCHAR(64) PRIMARY KEY, order_id VARCHAR(64) NOT NULL,
 version BIGINT NOT NULL CHECK(version > 0), updated_at DATETIME(3) NOT NULL,
 sku_id VARCHAR(64) NOT NULL, product_id VARCHAR(64) NOT NULL, category_id VARCHAR(64) NOT NULL,
 brand_id VARCHAR(64) NOT NULL, shop_id VARCHAR(64) NOT NULL,
 quantity INT NOT NULL CHECK(quantity > 0), unit_price_cent BIGINT NOT NULL CHECK(unit_price_cent > 0),
 discount_cent BIGINT NOT NULL CHECK(discount_cent >= 0), paid_cent BIGINT NOT NULL CHECK(paid_cent > 0),
 CHECK(paid_cent=quantity*unit_price_cent-discount_cent), FOREIGN KEY(order_id) REFERENCES order_header(id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS payment_attempt (
 id VARCHAR(64) PRIMARY KEY, order_id VARCHAR(64) NOT NULL,
 version BIGINT NOT NULL CHECK(version > 0), updated_at DATETIME(3) NOT NULL,
 channel VARCHAR(32) NOT NULL, status VARCHAR(16) NOT NULL CHECK(status IN ('PENDING','SUCCEEDED','FAILED')),
 amount_cent BIGINT NOT NULL CHECK(amount_cent > 0), failure_code VARCHAR(64),
 FOREIGN KEY(order_id) REFERENCES order_header(id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS payment_ledger (
 id VARCHAR(64) PRIMARY KEY, order_id VARCHAR(64) NOT NULL UNIQUE,
 version BIGINT NOT NULL CHECK(version > 0), updated_at DATETIME(3) NOT NULL,
 attempt_id VARCHAR(64) NOT NULL UNIQUE, channel_transaction_id VARCHAR(100) NOT NULL UNIQUE,
 status VARCHAR(16) NOT NULL CHECK(status='SUCCEEDED'), amount_cent BIGINT NOT NULL CHECK(amount_cent > 0),
 paid_at DATETIME(3) NOT NULL, FOREIGN KEY(order_id) REFERENCES order_header(id),
 FOREIGN KEY(attempt_id) REFERENCES payment_attempt(id), KEY idx_paid(paid_at)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS payment_line (
 id VARCHAR(64) PRIMARY KEY, order_id VARCHAR(64) NOT NULL,
 version BIGINT NOT NULL CHECK(version > 0), updated_at DATETIME(3) NOT NULL,
 payment_id VARCHAR(64) NOT NULL, order_line_id VARCHAR(64) NOT NULL UNIQUE,
 amount_cent BIGINT NOT NULL CHECK(amount_cent > 0),
 FOREIGN KEY(order_id) REFERENCES order_header(id), FOREIGN KEY(payment_id) REFERENCES payment_ledger(id),
 FOREIGN KEY(order_line_id) REFERENCES order_line(id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS refund_header (
 id VARCHAR(64) PRIMARY KEY, order_id VARCHAR(64) NOT NULL,
 version BIGINT NOT NULL CHECK(version > 0), updated_at DATETIME(3) NOT NULL,
 payment_id VARCHAR(64) NOT NULL, status VARCHAR(16) NOT NULL,
 amount_cent BIGINT NOT NULL CHECK(amount_cent > 0), line_count INT NOT NULL CHECK(line_count > 0),
 succeeded_at DATETIME(3), FOREIGN KEY(order_id) REFERENCES order_header(id),
 FOREIGN KEY(payment_id) REFERENCES payment_ledger(id),
 CHECK(status IN ('APPLIED','PROCESSING','SUCCEEDED','FAILED','CLOSED')),
 CHECK(status<>'SUCCEEDED' OR succeeded_at IS NOT NULL), KEY idx_refund(succeeded_at)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS refund_line (
 id VARCHAR(64) PRIMARY KEY, order_id VARCHAR(64) NOT NULL,
 version BIGINT NOT NULL CHECK(version > 0), updated_at DATETIME(3) NOT NULL,
 refund_id VARCHAR(64) NOT NULL, order_line_id VARCHAR(64) NOT NULL, amount_cent BIGINT NOT NULL CHECK(amount_cent > 0),
 UNIQUE KEY uq_refund_line(refund_id,order_line_id), FOREIGN KEY(order_id) REFERENCES order_header(id),
 FOREIGN KEY(refund_id) REFERENCES refund_header(id), FOREIGN KEY(order_line_id) REFERENCES order_line(id)
) ENGINE=InnoDB;
-- Fixture dimension rows are initialized by MySqlSimulator, not by CDC.
