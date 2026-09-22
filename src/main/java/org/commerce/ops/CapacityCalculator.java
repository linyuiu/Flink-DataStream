package org.commerce.ops;

import org.commerce.config.CommerceConfig;

/** Planning assumptions, not a throughput guarantee. */
public final class CapacityCalculator {
    public static void main(String[] args) throws Exception {
        CommerceConfig config = CommerceConfig.load(args);
        double ordersPerDay = config.positive("capacity.orders.per.day"),
                annualGmvYuan = config.positive("capacity.annual.gmv.yuan");
        double paymentRate = Double.parseDouble(config.get("business.payment.rate")),
                peakFactor = config.positive("capacity.peak.factor");
        // PROFILE: 5% one refund + 5% two partial refunds + 5% two refunds closing the goods
        // balance.
        double averageOrdersPerSecond = ordersPerDay / 86400,
                expectedEvents =
                        ordersPerDay
                                * (1 + (1 - paymentRate) * .8 + paymentRate + paymentRate * .25);
        System.out.printf(
                java.util.Locale.ROOT,
                "orders/day=%.0f avg_orders/s=%.2f peak_orders/s=%.2f required_paid_AOV_yuan=%.2f%n"
                        + "assumed_outbox_events/day=%.0f peak_events/s=%.2f budget_expanded_records/s=%.2f%n"
                        + "raw_changes/day_assuming_16_per_order=%.0f event_payload_GB/day_assuming_3KB=%.2f%n",
                ordersPerDay,
                averageOrdersPerSecond,
                averageOrdersPerSecond * peakFactor,
                annualGmvYuan / (365 * ordersPerDay * paymentRate),
                expectedEvents,
                expectedEvents / 86400 * peakFactor,
                expectedEvents / 86400 * peakFactor * 20,
                ordersPerDay * 16,
                expectedEvents * 3000 / 1e9);
    }
}
