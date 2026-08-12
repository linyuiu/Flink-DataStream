package org.linyu.validation;

import org.linyu.map.OrderDetailRealTime;

import java.math.BigDecimal;
import java.util.Locale;

public final class OrderSnapshotValidator {
    private OrderSnapshotValidator() {
    }

    public static void validate(OrderDetailRealTime order) {
        if (order == null) {

            throw new OrderValidationException(
                    "订单对象不能为空"
            );
        }
        if (isBlank(order.orderId)) {
            throw new OrderValidationException(
                    "order_id 不能为空"
            );
        }
        if (isBlank(order.orderStatus)) {
            throw new OrderValidationException(
                    "order_status不能为空"
            );
        }

        if (isBlank(order.updateTime)) {
            throw new OrderValidationException(
                    "update_time不能为空"
            );
        }

        String status = order.orderStatus
                .trim()
                .toUpperCase(Locale.ROOT);

        switch (status) {
            case "UNPAID":
            case "CANCELLED":
            case "CANCELED":
            case "CLOSED":
                return;

            case "PAID":
            case "PART_REFUNDED":
            case "PARTIALLY_REFUNDED":
            case "REFUNDED":
                validatePaidSnapshot(order, status);
                return;

            default:
                throw new OrderValidationException(
                        "不支持的订单状态：" + status
                );
        }


    }

    private static void validatePaidSnapshot(OrderDetailRealTime order, String status) {
        if (order.payAmount == null) {
            throw new OrderValidationException(
                    status + "状态下pay_amount不能为空"
            );
        }

        if (order.refundAmount == null) {
            throw new OrderValidationException(
                    status + "状态下refund_amount不能为空"
            );
        }

        if (order.payAmount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new OrderValidationException(
                    "pay_amount必须大于0"
            );
        }

        if (order.refundAmount.compareTo(BigDecimal.ZERO) < 0) {
            throw new OrderValidationException(
                    "refund_amount不能小于0"
            );
        }

        if (order.refundAmount.compareTo(order.payAmount) > 0) {
            throw new OrderValidationException(
                    "refund_amount不能大于pay_amount"
            );
        }

        if (isBlank(order.payTime)) {
            throw new OrderValidationException(
                    status + "状态下pay_time不能为空"
            );
        }
        validateStatusAmountRelation(order, status);
    }


    private static void validateStatusAmountRelation(
            OrderDetailRealTime order,
            String status
    ) {
        switch (status) {
            case "PAID":
                if (order.refundAmount.compareTo(BigDecimal.ZERO) != 0) {
                    throw new OrderValidationException(
                            "PAID状态下refund_amount必须为0"
                    );
                }
                break;

            case "PART_REFUNDED":
            case "PARTIALLY_REFUNDED":
                if (order.refundAmount.compareTo(BigDecimal.ZERO) <= 0
                        || order.refundAmount.compareTo(order.payAmount) >= 0) {
                    throw new OrderValidationException(
                            "部分退款金额必须大于0且小于pay_amount"
                    );
                }
                break;

            case "REFUNDED":
                if (order.refundAmount.compareTo(order.payAmount) != 0) {
                    throw new OrderValidationException(
                            "全额退款时refund_amount必须等于pay_amount"
                    );
                }
                break;

            default:
                break;
        }
    }

    private static boolean isBlank(String string) {
        return string == null || string.trim().isEmpty();
    }
}
