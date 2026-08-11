package org.linyu.validation;

import org.linyu.map.OrderDetailRealTime;

public final class OrderSnapshotValidator {
    private OrderSnapshotValidator() {
    }

    public static void validate(OrderDetailRealTime orderDetailRealTime) {
        if (orderDetailRealTime == null) {

            throw new OrderValidationException(
                    "订单对象不能为空"
            );
        }


    }

    private static boolean isBlank(String string) {
        return string == null || string.trim().isEmpty();
    }
}
