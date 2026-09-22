-- DORIS: never sum across dimension_type; order/user counts are not additive across dimension members/days.
-- Default split deployment: integrated deployment uses dashboard / metric_day instead.
SELECT * FROM commerce.dashboard_v2 WHERE dimension_type='ALL' ORDER BY biz_date DESC;
SELECT biz_date,dimension_type,SUM(paid_goods_cent),SUM(refund_goods_cent),SUM(net_paid_goods_cent)
FROM commerce.metric_day_v2 GROUP BY biz_date,dimension_type;
SELECT event_date,event_type,COUNT(*) FROM commerce.trade_event GROUP BY event_date,event_type;

-- MYSQL: run against source AFTER CDC catches up; times are epoch millis, use an explicit +08:00 session.
-- SET time_zone='+08:00';
-- SELECT DATE(FROM_UNIXTIME(p.paid_at/1000)),COUNT(DISTINCT p.order_id),SUM(a.amount_cent)
-- FROM pay_ledger p JOIN pay_allocation a ON a.payment_id=p.id GROUP BY DATE(FROM_UNIXTIME(p.paid_at/1000));
-- SELECT DATE(FROM_UNIXTIME(r.succeeded_at/1000)),COUNT(DISTINCT r.id),SUM(a.amount_cent)
-- FROM refund_request r JOIN refund_allocation a ON a.refund_id=r.id WHERE r.status='SUCCEEDED'
-- GROUP BY DATE(FROM_UNIXTIME(r.succeeded_at/1000));
-- Independent over-refund audit (must return zero rows):
-- SELECT a.order_line_id,SUM(a.amount_cent) refunded,MAX(p.amount_cent) paid
-- FROM refund_allocation a JOIN refund_request r ON r.id=a.refund_id AND r.status='SUCCEEDED'
-- JOIN pay_allocation p ON p.order_line_id=a.order_line_id GROUP BY a.order_line_id HAVING refunded>paid;
-- Missing outbox checks: successful payment/refund must have exactly one corresponding business event.
-- SELECT p.id FROM pay_ledger p LEFT JOIN trade_outbox o ON o.event_type='PAYMENT_SUCCEEDED'
-- AND o.order_id=p.order_id AND o.business_id=p.id WHERE o.id IS NULL;
