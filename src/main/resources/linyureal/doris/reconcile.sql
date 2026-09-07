-- Wait for all three jobs' checkpoints and input lag to settle before comparing.
-- Payment date net GMV. Compare each dimension separately; NEVER sum across dimension_type.
SELECT pay_date, product_id,
 SUM(CASE WHEN fact_type='PAYMENT' THEN amount_cent ELSE -amount_cent END)/100.0 AS expected_net
FROM linyureal.dwd_money_item_fact GROUP BY pay_date,product_id;

SELECT m.*, COALESCE(d.display_name,m.dimension_id) AS dimension_name
FROM linyureal.ads_gmv_dimension_day m
LEFT JOIN linyureal.dim_catalog d ON m.dimension_type=d.dimension_type AND m.dimension_id=d.dimension_id
ORDER BY m.biz_date,m.dimension_type,m.dimension_id;

-- Compare monetary totals across each dimension family; PROVINCE/CITY/DISTRICT are separate families.
SELECT biz_date,dimension_type,SUM(paid_gmv),SUM(refund_amount),SUM(net_paid_gmv)
FROM linyureal.ads_gmv_dimension_day GROUP BY biz_date,dimension_type;

-- Independent expected values from SUCCESS payment/refund ledgers in MySQL:
-- SELECT DATE(p.pay_time),SUM(pi.amount_cent) FROM trade_payment p
-- JOIN trade_payment_item pi ON pi.payment_id=p.id GROUP BY DATE(p.pay_time);
-- SELECT DATE(r.refund_time),SUM(ri.amount_cent) FROM trade_refund r
-- JOIN trade_refund_item ri ON ri.refund_id=r.id WHERE r.status='SUCCEEDED' GROUP BY DATE(r.refund_time);
