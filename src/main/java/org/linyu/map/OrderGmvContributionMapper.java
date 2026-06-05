package org.linyu.map;

import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;

/**
 * 按交易时间 pay_time 判断今天，并计算订单当前对今日 GMV 的贡献。
 */
public class OrderGmvContributionMapper
        implements MapFunction<OrderEvent, OrderGmvContribution> {

    @Override
    public OrderGmvContribution map(OrderEvent event) {
        if (!event.isPositiveGmvStatus() || !event.isTodayTrade()) {
            return OrderGmvContribution.empty(event.orderId);
        }

        BigDecimal netGmv = event.netGmv();
        if (netGmv.compareTo(BigDecimal.ZERO) <= 0) {
            return OrderGmvContribution.empty(event.orderId);
        }

        return new OrderGmvContribution(
                event.orderId,
                event.tradeDate(),
                netGmv,
                1L
        );
    }
}
