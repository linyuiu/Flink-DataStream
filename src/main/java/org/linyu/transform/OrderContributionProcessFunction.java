package org.linyu.transform;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.linyu.config.EnumV;
import org.linyu.map.GmvDeltaRealTime;
import org.linyu.map.OrderContributionState;
import org.linyu.map.OrderDetailRealTime;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Locale;
import java.util.Objects;

/**
 * 订单贡献变化计算。
 * <p>
 * Key：
 * order_id
 * <p>
 * State：
 * 该订单上一次对GMV的贡献。
 */
public class OrderContributionProcessFunction
        extends KeyedProcessFunction<
        String,
        OrderDetailRealTime,
        GmvDeltaRealTime> {
    private transient ValueState<OrderContributionState>
            previousContributionState;

    @Override
    public void processElement(OrderDetailRealTime orderDetailRealTime,
                               Context context,
                               Collector<GmvDeltaRealTime> collector) throws Exception {

        OrderContributionState previousState =
                previousContributionState.value();

        /*
         * update_time 是推荐上游补充的字段。
         *
         * 有 update_time：
         * 可以识别迟到的旧版本订单。
         *
         * 没有 update_time：
         * 只能假设 Kafka 中同一 order_id
         * 按正确业务顺序到达。
         */

        Long currentVersion;

        try {
            currentVersion = parseVersion(orderDetailRealTime.updateTime);
        } catch (Exception exception) {
            context.output(
                    BUSINESS_DIRTY_TAG,
                    "order_id=" + orderDetailRealTime.orderId
                            + "update_time 格式错误："
                            + orderDetailRealTime.updateTime
            );
            return;
        }
        /*
         * 已经处理过有版本的数据，
         * 后续却收到无版本数据。
         *
         * 为防止旧数据覆盖新状态，这里拒绝处理。
         */

        if (previousState != null
                && previousState.lastVersion != null
                && currentVersion == null) {
            context.output(
                    BUSINESS_DIRTY_TAG,
                    "order_id=" + orderDetailRealTime.orderId
                            + "，历史数据有版本号，"
                            + "当前消息缺少update_time"
            );
            return;
        }
        /*
         * 当前版本早于或等于已经处理过的版本，
         * 认为是旧数据或重复数据，直接忽略。
         */

        if (previousState != null
                && previousState.lastVersion != null
                && currentVersion != null
                && currentVersion <= previousState.lastVersion) {
            return;
        }

        BigDecimal newContribution = calculateContribution(orderDetailRealTime);

        if (newContribution == null) {
            context.output(
                    BUSINESS_DIRTY_TAG,
                    "order_id=" + orderDetailRealTime.orderId
                            + "，无法识别的订单状态："
                            + orderDetailRealTime.orderStatus
                    );
            return;
        }

        BigDecimal oldContribution =
                previousState == null
                    ? BigDecimal.ZERO
                    : zeroIfNull(
                            previousState.contribution
        );

        String oldBizDate =
                previousState == null
                ? null
                        : previousState.bizDate;

        String newBizDate =
                extractNullableDate(
                        orderDetailRealTime.payTime
                );


        /*
         * 取消或退款事件可能没有pay_time。
         *
         * 已经存在历史状态时，
         * 使用原订单的业务日期。
         */
        if (newBizDate == null
                && previousState != null) {
            newBizDate = previousState.bizDate;
        }

        /*
         * 新贡献不为0，却没有业务日期，
         * 无法判断金额应该计入哪一天。
         */

        if (newContribution
                .compareTo(BigDecimal.ZERO) != 0
                && newBizDate == null) {

            context.output(
                    BUSINESS_DIRTY_TAG,
                    "order_id=" + orderDetailRealTime.orderId
                            + "，存在GMV贡献但pay_time为空"
            );

            return;
        }


        /*
         * 情况一：
         * 订单前后属于同一天。
         *
         * 只需要输出：
         * 新贡献 - 旧贡献
         */
        if (Objects.equals(
                oldBizDate,
                newBizDate)) {
            BigDecimal delta = newContribution.subtract(
                    oldContribution
            );

            emitDelta(
                    newBizDate,
                    delta,
                    orderDetailRealTime.orderId,
                    collector
            );

        } else {
            /*
            * 情况二：
            * 订单业务日期发生变化。
            * 从旧日期减掉旧恭喜
            * */
            if (oldBizDate != null
                    && oldContribution.compareTo(
                    BigDecimal.ZERO) != 0) {
                emitDelta(
                        oldBizDate,
                        oldContribution,
                        orderDetailRealTime.orderId,
                        collector
                );
            }
            /*
            *  给新日期增加新贡献
            *
            *
            * */
            if (newBizDate != null
                    && newContribution.compareTo(BigDecimal.ZERO) != 0) {
                emitDelta(
                        newBizDate,
                        newContribution,
                        orderDetailRealTime.orderId,
                        collector
                );
            }
        }

        Long nextVersion =
                currentVersion != null
                        ? currentVersion
                        : previousState == null
                        ? null
                        : previousState.lastVersion;
        /*
        * 更新这个订单的状态
        *
        * ValueState 会跟踪 checkpoint 保存*/

        previousContributionState.update(
                new OrderContributionState(
                        newBizDate,
                        newContribution,
                        nextVersion
                )
        );

    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        ValueStateDescriptor<OrderContributionState>
                stateDescriptor =
                new ValueStateDescriptor<>(
                "order-contribution-state",
                OrderContributionState.class
        );

        previousContributionState = getRuntimeContext().getState(
                stateDescriptor
        );
    }

    private static Long parseVersion(
            String updateTime) {

        if (isBlank(updateTime)) {
            return null;
        }

        LocalDateTime localDateTime =
                LocalDateTime.parse(
                        updateTime,
                        EnumV.DATE_TIME_FORMATTER
                );

        return localDateTime
                .atZone(EnumV.BUSINESS_ZONE)
                .toInstant()
                .toEpochMilli();
    }

    private static boolean isBlank(String value) {
        return value == null
                || value.trim().isEmpty();
    }
    private static final OutputTag<String> BUSINESS_DIRTY_TAG =
            new OutputTag<String>("business-dirty-data") {
            };


    /**
     * 根据订单当前状态，
     * 计算这个订单现在应该对GMV贡献多少钱。
     */
    private BigDecimal calculateContribution(
            OrderDetailRealTime order) {

        String status =
                order.orderStatus
                        .trim()
                        .toUpperCase(Locale.ROOT);

        BigDecimal payAmount =
                zeroIfNull(order.payAmount);

        BigDecimal refundAmount =
                zeroIfNull(order.refundAmount);

        switch (status) {

            /*
             * 未支付、取消、关闭订单不产生GMV贡献。
             */
            case "UNPAID":
            case "CANCELLED":
            case "CANCELED":
            case "CLOSED":
                return BigDecimal.ZERO;

            /*
             * 已支付和退款状态。
             */
            case "PAID":
            case "PART_REFUNDED":
            case "PARTIALLY_REFUNDED":
            case "REFUNDED":

                if (!EnumV.DEDUCT_REFUNDS) {
                    return payAmount;
                }

                BigDecimal netAmount =
                        payAmount.subtract(
                                refundAmount
                        );

                /*
                 * 防止脏数据导致净金额小于0。
                 */
                return netAmount.max(
                        BigDecimal.ZERO
                );

            default:
                return null;
        }
    }

    private static BigDecimal zeroIfNull(
            BigDecimal amount) {

        return amount == null
                ? BigDecimal.ZERO
                : amount;
    }

    private static String extractNullableDate(
            String dateTime) {

        if (isBlank(dateTime)) {
            return null;
        }

        if (dateTime.length() < 10) {
            throw new IllegalArgumentException(
                    "时间格式错误：" + dateTime
            );
        }

        return dateTime.substring(0, 10);
    }

    private void emitDelta(
            String bizDate,
            BigDecimal delta,
            String orderId,
            Collector<GmvDeltaRealTime> out) {

        if (bizDate == null
                || delta == null
                || delta.compareTo(
                BigDecimal.ZERO) == 0) {

            return;
        }

        out.collect(
                new GmvDeltaRealTime(
                        bizDate,
                        delta,
                        orderId
                )
        );
    }
}
