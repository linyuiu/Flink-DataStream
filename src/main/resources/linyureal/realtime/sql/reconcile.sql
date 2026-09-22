-- DORIS: compare only after input catches up and checkpoints/Stream Load publish.
SELECT * FROM rt_trade.v_trade_dashboard WHERE dimension_type='ALL' ORDER BY biz_date;

-- Event-day amounts and distinct counts reconstructed independently from DWD.
SELECT event_date,
 COUNT(DISTINCT CASE WHEN fact_type='CREATED' THEN order_id END) AS created_orders,
 COUNT(DISTINCT CASE WHEN fact_type='CANCELLED' THEN order_id END) AS cancelled_orders,
 COUNT(DISTINCT CASE WHEN fact_type='PAID' THEN order_id END) AS paid_orders,
 COUNT(DISTINCT CASE WHEN fact_type='PAID' THEN user_id END) AS paid_users,
 COUNT(DISTINCT CASE WHEN fact_type='REFUNDED' THEN order_id END) AS refund_orders,
 COUNT(DISTINCT CASE WHEN fact_type='REFUNDED' THEN business_id END) AS refund_requests,
 SUM(CASE WHEN fact_type='PAID' THEN amount_cent ELSE 0 END) AS paid_cent,
 SUM(CASE WHEN fact_type='REFUNDED' THEN amount_cent ELSE 0 END) AS refund_cent
FROM rt_trade.dwd_trade_item_fact GROUP BY event_date;

SELECT pay_date, SUM(CASE WHEN fact_type='PAID' THEN amount_cent ELSE -amount_cent END) AS net_paid_cent
FROM rt_trade.dwd_trade_item_fact WHERE fact_type IN ('PAID','REFUNDED') GROUP BY pay_date;

-- Amounts are additive within a dimension family; distinct order/user counts generally are NOT.
SELECT biz_date,dimension_type,SUM(paid_amount_cent),SUM(refund_amount_cent),SUM(net_paid_amount_cent)
FROM rt_trade.ads_trade_metrics_day GROUP BY biz_date,dimension_type;

-- MYSQL (execute separately against source DB):
-- SELECT DATE(p.paid_at),COUNT(DISTINCT p.order_id),SUM(l.amount_cent)
-- FROM payment_ledger p JOIN payment_line l ON l.payment_id=p.id GROUP BY DATE(p.paid_at);
-- SELECT DATE(r.succeeded_at),COUNT(DISTINCT r.id),SUM(l.amount_cent)
-- FROM refund_header r JOIN refund_line l ON l.refund_id=r.id WHERE r.status='SUCCEEDED' GROUP BY DATE(r.succeeded_at);
-- SELECT SUM(amount_cent) FROM payment_ledger; -- includes freight, NOT equal to goods GMV
