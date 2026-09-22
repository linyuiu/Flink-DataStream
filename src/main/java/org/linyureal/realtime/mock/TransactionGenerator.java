package org.linyureal.realtime.mock;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.linyureal.realtime.common.Jsons;
import org.linyureal.realtime.model.RowChange;
import java.time.LocalDateTime;
import java.util.*;

/** One inner list = one local database transaction. No independently randomized foreign keys. */
public final class TransactionGenerator {
    private TransactionGenerator() {}
    public static List<List<RowChange>> generate(String id, Scenario scenario, LocalDateTime created, Random random) {
        List<List<RowChange>> transactions = new ArrayList<>();
        String shop = random.nextBoolean() ? "S1" : "S2"; boolean shanghai = random.nextBoolean();
        ObjectNode order = row(id, id, created).put("user_id", "U" + (1 + random.nextInt(30))).put("shop_id", shop);
        order.put("currency", "CNY").put("status", "CREATED").put("created_at", time(created)).putNull("cancelled_at");
        order.put("province_id", shanghai ? "310000" : "330000").put("city_id", shanghai ? "310100" : "330100")
                .put("district_id", shanghai ? "310115" : "330106");
        int q1 = 1 + random.nextInt(3), q2 = 1 + random.nextInt(2);
        // Order-level coupon allocated once by the transaction service, preserving every cent.
        long gross1 = 8900L * q1, gross2 = 12900L * q2, coupon = random.nextBoolean() ? 1000 : 0;
        long discount1 = coupon * gross1 / (gross1 + gross2), discount2 = coupon - discount1;
        long a = gross1 - discount1, b = gross2 - discount2, freight = a + b >= 30000 ? 0 : 800;
        ObjectNode l1 = line(id + "-L1", id, "SKU1", "P1", "C1", "B1", shop, q1, 8900, discount1, created);
        ObjectNode l2 = line(id + "-L2", id, "SKU2", "P2", "C2", "B2", shop, q2, 12900, discount2, created);
        order.put("line_count", 2).put("goods_cent", a + b).put("freight_cent", freight).put("payable_cent", a + b + freight);
        transactions.add(List.of(change("order_header", order), change("order_line", l1), change("order_line", l2)));
        if (scenario == Scenario.CANCELLED) {
            ObjectNode cancelled = next(order, created.plusMinutes(1)).put("status", "CANCELLED")
                    .put("cancelled_at", time(created.plusMinutes(1)));
            transactions.add(List.of(change("order_header", cancelled))); return transactions;
        }
        LocalDateTime paid = created.plusMinutes(2);
        if (scenario == Scenario.PAYMENT_FAILED || scenario == Scenario.RETRY_PAID) {
            attempt(transactions, id, "A1", a + b + freight, created.plusSeconds(10), false);
            if (scenario == Scenario.PAYMENT_FAILED) return transactions;
        }
        String attemptSuffix = scenario == Scenario.RETRY_PAID ? "A2" : "A1";
        ObjectNode attempt = row(id + "-" + attemptSuffix, id, paid.minusSeconds(10))
                .put("channel", "MOCK_PAY").put("status", "PENDING").put("amount_cent", a + b + freight).putNull("failure_code");
        transactions.add(List.of(change("payment_attempt", attempt)));
        ObjectNode success = next(attempt, paid).put("status", "SUCCEEDED");
        ObjectNode ledger = row(id + "-PAY", id, paid).put("attempt_id", Jsons.text(attempt, "id"))
                .put("channel_transaction_id", id + "-CHANNEL").put("status", "SUCCEEDED")
                .put("amount_cent", a + b + freight).put("paid_at", time(paid));
        transactions.add(List.of(change("payment_attempt", success), change("payment_ledger", ledger),
                change("payment_line", allocation(id + "-PL1", id, id + "-L1", a, paid)),
                change("payment_line", allocation(id + "-PL2", id, id + "-L2", b, paid)),
                change("order_header", next(order, paid).put("status", "PAID"))));
        if (scenario == Scenario.PAID || scenario == Scenario.RETRY_PAID) return transactions;
        LocalDateTime refunded = scenario == Scenario.CROSS_DAY_REFUND ? paid.plusDays(1) : paid.plusMinutes(1);
        long first = a / 3;
        refund(transactions, id, "R1", first, 0, refunded, scenario != Scenario.REFUND_FAILED);
        if (scenario == Scenario.MULTIPLE_REFUNDS) refund(transactions, id, "R2", a / 4, 0, refunded.plusMinutes(1), true);
        if (scenario == Scenario.FULL_GOODS_REFUND) refund(transactions, id, "R2", a - first, b, refunded.plusMinutes(1), true);
        return transactions;
    }
    private static void attempt(List<List<RowChange>> tx, String id, String suffix, long amount, LocalDateTime t, boolean success) {
        ObjectNode a = row(id + "-" + suffix, id, t).put("channel", "MOCK_PAY").put("status", "PENDING")
                .put("amount_cent", amount).putNull("failure_code");
        tx.add(List.of(change("payment_attempt", a)));
        tx.add(List.of(change("payment_attempt", next(a, t.plusSeconds(5)).put("status", success ? "SUCCEEDED" : "FAILED")
                .put("failure_code", success ? null : "INSUFFICIENT_FUNDS"))));
    }
    private static void refund(List<List<RowChange>> tx, String order, String suffix, long a, long b, LocalDateTime t, boolean ok) {
        String id = order + "-" + suffix;
        ObjectNode header = row(id, order, t).put("payment_id", order + "-PAY").put("status", "APPLIED")
                .put("amount_cent", a + b).put("line_count", b > 0 ? 2 : 1).putNull("succeeded_at");
        List<RowChange> apply = new ArrayList<>(); apply.add(change("refund_header", header));
        apply.add(change("refund_line", row(id + "-RL1", order, t).put("refund_id", id)
                .put("order_line_id", order + "-L1").put("amount_cent", a)));
        if (b > 0) apply.add(change("refund_line", row(id + "-RL2", order, t).put("refund_id", id)
                .put("order_line_id", order + "-L2").put("amount_cent", b)));
        tx.add(apply);
        ObjectNode processing = next(header, t.plusSeconds(5)).put("status", "PROCESSING");
        tx.add(List.of(change("refund_header", processing)));
        ObjectNode result = next(processing, t.plusSeconds(10)).put("status", ok ? "SUCCEEDED" : "FAILED");
        if (ok) result.put("succeeded_at", time(t.plusSeconds(10)));
        tx.add(List.of(change("refund_header", result)));
    }
    private static ObjectNode line(String id, String order, String sku, String product, String category, String brand,
                                   String shop, int qty, long price, long discount, LocalDateTime t) {
        return row(id, order, t).put("sku_id", sku).put("product_id", product).put("category_id", category)
                .put("brand_id", brand).put("shop_id", shop).put("quantity", qty).put("unit_price_cent", price)
                .put("discount_cent", discount).put("paid_cent", price * qty - discount);
    }
    private static ObjectNode allocation(String id, String order, String line, long amount, LocalDateTime t) {
        return row(id, order, t).put("payment_id", order + "-PAY").put("order_line_id", line).put("amount_cent", amount);
    }
    private static ObjectNode row(String id, String order, LocalDateTime t) {
        return Jsons.MAPPER.createObjectNode().put("id", id).put("order_id", order).put("version", 1).put("updated_at", time(t));
    }
    private static ObjectNode next(ObjectNode row, LocalDateTime t) {
        return row.deepCopy().put("version", Jsons.integer(row, "version") + 1).put("updated_at", time(t));
    }
    private static RowChange change(String table, ObjectNode row) {
        return new RowChange(table, Jsons.integer(row, "version") == 1 ? "c" : "u", Jsons.text(row, "order_id"), row.toString());
    }
    private static String time(LocalDateTime t) { return t.format(Jsons.TIME); }
}
