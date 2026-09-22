CREATE DATABASE IF NOT EXISTS commerce CHARACTER SET utf8mb4;
USE commerce;

CREATE TABLE IF NOT EXISTS trade_order (
 id VARCHAR(100) PRIMARY KEY, version BIGINT NOT NULL CHECK(version>0), checkout_id VARCHAR(100) NOT NULL,
 user_id VARCHAR(100) NOT NULL, shop_id VARCHAR(100) NOT NULL, province_id VARCHAR(20) NOT NULL,
 city_id VARCHAR(20) NOT NULL, district_id VARCHAR(20) NOT NULL,
 created_at BIGINT NOT NULL, cancelled_at BIGINT NOT NULL DEFAULT 0,
 status VARCHAR(16) NOT NULL CHECK(status IN ('CREATED','PAID','CANCELLED','COMPLETED')),
 currency CHAR(3) NOT NULL CHECK(currency='CNY'), line_count INT NOT NULL CHECK(line_count>0),
 goods_cent BIGINT NOT NULL CHECK(goods_cent>0), freight_cent BIGINT NOT NULL CHECK(freight_cent>=0),
 payable_cent BIGINT NOT NULL, CHECK(payable_cent=goods_cent+freight_cent),
 KEY idx_checkout(checkout_id), KEY idx_created(created_at)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS trade_order_line (
 id VARCHAR(100) PRIMARY KEY, version BIGINT NOT NULL, order_id VARCHAR(100) NOT NULL,
 sku_id VARCHAR(100) NOT NULL, product_id VARCHAR(100) NOT NULL, category_id VARCHAR(100) NOT NULL, brand_id VARCHAR(100) NOT NULL,
 quantity INT NOT NULL CHECK(quantity>0), unit_price_cent BIGINT NOT NULL CHECK(unit_price_cent>0),
 discount_cent BIGINT NOT NULL CHECK(discount_cent>=0), payable_cent BIGINT NOT NULL CHECK(payable_cent>0),
 CHECK(payable_cent=quantity*unit_price_cent-discount_cent), FOREIGN KEY(order_id) REFERENCES trade_order(id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS pay_attempt (
 id VARCHAR(100) PRIMARY KEY, version BIGINT NOT NULL, order_id VARCHAR(100) NOT NULL,
 amount_cent BIGINT NOT NULL CHECK(amount_cent>0), status VARCHAR(16) NOT NULL CHECK(status IN ('PENDING','FAILED','SUCCEEDED')),
 failure_code VARCHAR(100) NOT NULL, FOREIGN KEY(order_id) REFERENCES trade_order(id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS pay_ledger (
 id VARCHAR(100) PRIMARY KEY, version BIGINT NOT NULL, order_id VARCHAR(100) NOT NULL UNIQUE,
 attempt_id VARCHAR(100) NOT NULL UNIQUE, channel_txn_id VARCHAR(100) NOT NULL UNIQUE,
 paid_at BIGINT NOT NULL CHECK(paid_at>0), amount_cent BIGINT NOT NULL CHECK(amount_cent>0),
 FOREIGN KEY(order_id) REFERENCES trade_order(id), FOREIGN KEY(attempt_id) REFERENCES pay_attempt(id), KEY idx_paid(paid_at)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS pay_allocation (
 id VARCHAR(100) PRIMARY KEY, version BIGINT NOT NULL, payment_id VARCHAR(100) NOT NULL,
 order_line_id VARCHAR(100) NOT NULL UNIQUE, amount_cent BIGINT NOT NULL CHECK(amount_cent>0),
 FOREIGN KEY(payment_id) REFERENCES pay_ledger(id), FOREIGN KEY(order_line_id) REFERENCES trade_order_line(id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS refund_request (
 id VARCHAR(100) PRIMARY KEY, version BIGINT NOT NULL, order_id VARCHAR(100) NOT NULL, payment_id VARCHAR(100) NOT NULL,
 amount_cent BIGINT NOT NULL CHECK(amount_cent>0), status VARCHAR(16) NOT NULL CHECK(status IN ('APPLIED','PROCESSING','SUCCEEDED','FAILED','CLOSED')),
 succeeded_at BIGINT NOT NULL DEFAULT 0, CHECK(status<>'SUCCEEDED' OR succeeded_at>0),
 FOREIGN KEY(order_id) REFERENCES trade_order(id), FOREIGN KEY(payment_id) REFERENCES pay_ledger(id), KEY idx_refund(succeeded_at)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS refund_allocation (
 id VARCHAR(100) PRIMARY KEY, version BIGINT NOT NULL, refund_id VARCHAR(100) NOT NULL,
 order_line_id VARCHAR(100) NOT NULL, amount_cent BIGINT NOT NULL CHECK(amount_cent>0),
 UNIQUE KEY uq_refund_line(refund_id,order_line_id), FOREIGN KEY(refund_id) REFERENCES refund_request(id),
 FOREIGN KEY(order_line_id) REFERENCES trade_order_line(id)
) ENGINE=InnoDB;

-- Transactional outbox: service inserts exactly once IN the business transaction, never edits payloads.
CREATE TABLE IF NOT EXISTS trade_outbox (
 id VARCHAR(100) PRIMARY KEY, order_id VARCHAR(100) NOT NULL, business_id VARCHAR(100) NOT NULL,
 event_type VARCHAR(24) NOT NULL CHECK(event_type IN ('ORDER_CREATED','ORDER_CANCELLED','PAYMENT_SUCCEEDED','REFUND_SUCCEEDED')),
 occurred_at BIGINT NOT NULL CHECK(occurred_at>0), payload_json JSON NOT NULL,
 UNIQUE KEY uq_business_event(event_type,order_id,business_id), KEY idx_occurred(occurred_at)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_category (
 id VARCHAR(100) PRIMARY KEY, name VARCHAR(200) NOT NULL, parent_id VARCHAR(100) NOT NULL,
 version BIGINT NOT NULL CHECK(version>0), attributes_json JSON NOT NULL
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS dim_brand LIKE dim_category;
CREATE TABLE IF NOT EXISTS dim_shop LIKE dim_category;
CREATE TABLE IF NOT EXISTS dim_product LIKE dim_category;
CREATE TABLE IF NOT EXISTS dim_sku LIKE dim_category;
CREATE TABLE IF NOT EXISTS dim_region (
 id VARCHAR(100) PRIMARY KEY, name VARCHAR(200) NOT NULL, parent_id VARCHAR(100) NOT NULL,
 version BIGINT NOT NULL CHECK(version>0), attributes_json JSON NOT NULL,
 region_level VARCHAR(12) NOT NULL CHECK(region_level IN ('PROVINCE','CITY','DISTRICT'))
) ENGINE=InnoDB;
-- Production: archive/purge under a governed policy; do NOT enable automatic outbox deletion before CDC/archive verification.
