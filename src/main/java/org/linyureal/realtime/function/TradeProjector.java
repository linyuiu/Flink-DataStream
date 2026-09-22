package org.linyureal.realtime.function;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.linyureal.realtime.common.Jsons;
import org.linyureal.realtime.model.*;
import org.linyureal.realtime.validation.RowValidator;
import java.util.*;
import static org.linyureal.realtime.common.Jsons.*;
import static org.linyureal.realtime.validation.RowValidator.require;

/** Pure order-grain projection. Call with a copy of committed state; discard it on validation failure. */
public final class TradeProjector {
    private TradeProjector() {}
    public static List<TradeFact> accept(OrderState state, RowChange change) {
        RowValidator.validate(change);
        ObjectNode row = object(change.rowJson);
        String rowKey = change.table + ":" + text(row, "id");
        String previous = state.rows.get(rowKey);
        if (previous != null) {
            ObjectNode old = object(previous);
            long version = integer(row, "version"), oldVersion = integer(old, "version");
            if (version < oldVersion) return List.of();
            if (version == oldVersion) {
                require(old.equals(row), "Same row version changed: " + rowKey); return List.of();
            }
            if (!List.of("order_header", "payment_attempt", "refund_header").contains(change.table)) {
                ObjectNode a = old.deepCopy(), b = row.deepCopy();
                a.remove(List.of("version", "updated_at")); b.remove(List.of("version", "updated_at"));
                require(a.equals(b), "Immutable business row changed: " + rowKey);
            }
            if (change.table.equals("refund_header") && "SUCCEEDED".equals(text(old, "status")))
                require("SUCCEEDED".equals(text(row, "status")), "Successful refund regressed");
            if (change.table.equals("payment_attempt") && !"PENDING".equals(text(old, "status")))
                require(text(old, "status").equals(text(row, "status")), "Terminal attempt changed; retry needs a new ID");
        }
        for (String stored : state.rows.values())
            require(text(object(stored), "order_id").equals(change.orderId), "Mixed order keys");
        state.rows.put(rowKey, row.toString());
        Map<String, ObjectNode> orders = rows(state, "order_header"), lines = rows(state, "order_line");
        require(orders.size() <= 1, "Multiple order headers");
        if (orders.isEmpty()) return List.of();
        ObjectNode order = orders.values().iterator().next();
        long expected = integer(order, "line_count");
        require(lines.size() <= expected, "Too many order lines");
        if (lines.size() != expected) return List.of();
        long goods = 0;
        for (ObjectNode line : lines.values()) {
            require(text(line, "shop_id").equals(text(order, "shop_id")), "Cross-shop suborder");
            goods = Math.addExact(goods, integer(line, "paid_cent"));
        }
        require(goods == integer(order, "goods_cent"), "Order/line sum mismatch");
        List<TradeFact> candidates = new ArrayList<>();
        for (ObjectNode line : lines.values()) {
            candidates.add(fact("CREATED", text(line, "id"), text(order, "id"), order, line,
                    time(order, "created_at").toLocalDate().toString(), "", integer(line, "paid_cent")));
            if ("CANCELLED".equals(text(order, "status"))) {
                require(!time(order, "cancelled_at").isBefore(time(order, "created_at")), "Cancellation before creation");
                candidates.add(fact("CANCELLED", text(line, "id"), text(order, "id"), order, line,
                        time(order, "cancelled_at").toLocalDate().toString(), "", integer(line, "paid_cent")));
            }
        }
        Map<String, ObjectNode> payments = rows(state, "payment_ledger");
        require(payments.size() <= 1, "This contract supports one full payment per suborder");
        if (!payments.isEmpty()) {
            require(!"CANCELLED".equals(text(order, "status")), "Cancelled unpaid order has successful payment");
            ObjectNode payment = payments.values().iterator().next();
            require(integer(payment, "amount_cent") == integer(order, "payable_cent"), "Payment/order mismatch");
            require(!time(payment, "paid_at").isBefore(time(order, "created_at")), "Payment before creation");
            ObjectNode attempt = rows(state, "payment_attempt").get(text(payment, "attempt_id"));
            Map<String, ObjectNode> allocations = rows(state, "payment_line");
            require(allocations.size() <= expected, "Too many payment allocations");
            if (attempt != null && "SUCCEEDED".equals(text(attempt, "status")) && allocations.size() == expected) {
                require(integer(attempt, "amount_cent") == integer(payment, "amount_cent"), "Attempt/payment mismatch");
                String payId = text(payment, "id"), payDate = time(payment, "paid_at").toLocalDate().toString();
                Set<String> allocated = new HashSet<>();
                for (ObjectNode allocation : allocations.values()) {
                    String lineId = text(allocation, "order_line_id");
                    ObjectNode line = lines.get(lineId);
                    require(line != null && allocated.add(lineId), "Invalid/duplicate payment line");
                    require(payId.equals(text(allocation, "payment_id")), "Wrong payment reference");
                    require(integer(line, "paid_cent") == integer(allocation, "amount_cent"), "Payment line amount mismatch");
                    candidates.add(fact("PAID", text(allocation, "id"), payId, order, line,
                            payDate, payDate, integer(allocation, "amount_cent")));
                }
                appendRefunds(state, candidates, order, lines, payment, payDate);
            } else if (attempt != null && "FAILED".equals(text(attempt, "status"))) {
                throw new IllegalArgumentException("Successful ledger references failed attempt");
            }
        }
        // Do not partially commit output markers if any projection conflicts.
        for (TradeFact f : candidates) {
            String old = state.emitted.get(f.fact_id);
            require(old == null || old.equals(write(f)), "Published fact changed; audited correction required: " + f.fact_id);
        }
        List<TradeFact> output = new ArrayList<>();
        for (TradeFact f : candidates) if (!state.emitted.containsKey(f.fact_id)) {
            state.emitted.put(f.fact_id, write(f)); output.add(f);
        }
        return output;
    }
    private static void appendRefunds(OrderState state, List<TradeFact> output, ObjectNode order,
                                      Map<String, ObjectNode> lines, ObjectNode payment, String payDate) {
        Map<String, Long> totals = new HashMap<>();
        for (ObjectNode refund : rows(state, "refund_header").values()) {
            require(text(refund, "payment_id").equals(text(payment, "id")), "Wrong refund payment");
            List<ObjectNode> details = new ArrayList<>();
            for (ObjectNode r : rows(state, "refund_line").values())
                if (text(r, "refund_id").equals(text(refund, "id"))) details.add(r);
            require(details.size() <= integer(refund, "line_count"), "Too many refund lines");
            if (!"SUCCEEDED".equals(text(refund, "status")) || details.size() != integer(refund, "line_count")) continue;
            require(!time(refund, "succeeded_at").isBefore(time(payment, "paid_at")), "Refund before payment");
            long sum = 0; Set<String> unique = new HashSet<>();
            for (ObjectNode detail : details) {
                String lineId = text(detail, "order_line_id"); ObjectNode line = lines.get(lineId);
                require(line != null && unique.add(lineId), "Invalid/duplicate refund line");
                long amount = integer(detail, "amount_cent"); sum = Math.addExact(sum, amount);
                long cumulative = Math.addExact(totals.getOrDefault(lineId, 0L), amount);
                require(cumulative <= integer(line, "paid_cent"), "Cumulative over-refund"); totals.put(lineId, cumulative);
                output.add(fact("REFUNDED", text(detail, "id"), text(refund, "id"), order, line,
                        time(refund, "succeeded_at").toLocalDate().toString(), payDate, amount));
            }
            require(sum == integer(refund, "amount_cent"), "Refund header/detail mismatch");
        }
    }
    /** Detect incomplete joins even when the order's PAID update itself has not arrived. */
    public static String incompleteReason(OrderState state) {
        Map<String, ObjectNode> orders = rows(state, "order_header");
        if (orders.isEmpty()) return "Missing order header";
        ObjectNode order = orders.values().iterator().next(); long expected = integer(order, "line_count");
        if (rows(state, "order_line").size() != expected) return "Missing order lines";
        Map<String, ObjectNode> payments = rows(state, "payment_ledger");
        Map<String, ObjectNode> attempts = rows(state, "payment_attempt");
        Map<String, ObjectNode> allocations = rows(state, "payment_line");
        Map<String, ObjectNode> refunds = rows(state, "refund_header");
        boolean needsPayment = List.of("PAID", "COMPLETED").contains(text(order, "status")) || !payments.isEmpty()
                || !allocations.isEmpty() || !refunds.isEmpty()
                || attempts.values().stream().anyMatch(a -> "SUCCEEDED".equals(text(a, "status")));
        if (needsPayment) {
            if (payments.isEmpty()) return "Missing successful payment ledger";
            ObjectNode payment = payments.values().iterator().next();
            ObjectNode attempt = attempts.get(text(payment, "attempt_id"));
            if (attempt == null || !"SUCCEEDED".equals(text(attempt, "status"))) return "Missing successful attempt";
            if (allocations.size() != expected) return "Missing payment allocations";
        }
        Map<String, ObjectNode> details = rows(state, "refund_line");
        for (ObjectNode detail : details.values())
            if (!refunds.containsKey(text(detail, "refund_id"))) return "Missing refund header";
        for (ObjectNode refund : refunds.values()) if ("SUCCEEDED".equals(text(refund, "status"))) {
            long count = details.values().stream().filter(d -> text(d, "refund_id").equals(text(refund, "id"))).count();
            if (count != integer(refund, "line_count")) return "Missing successful refund lines";
        }
        return null;
    }
    private static Map<String, ObjectNode> rows(OrderState state, String table) {
        Map<String, ObjectNode> result = new LinkedHashMap<>();
        state.rows.forEach((key, value) -> { if (key.startsWith(table + ":")) {
            ObjectNode row = object(value); result.put(text(row, "id"), row);
        }});
        return result;
    }
    private static TradeFact fact(String type, String id, String businessId, ObjectNode order, ObjectNode line,
                                   String date, String payDate, long amount) {
        TradeFact f = new TradeFact();
        f.fact_id = type + ":" + id; f.fact_type = type; f.business_id = businessId;
        f.order_id = text(order, "id"); f.order_line_id = text(line, "id"); f.user_id = text(order, "user_id");
        f.product_id = text(line, "product_id"); f.sku_id = text(line, "sku_id");
        f.category_id = text(line, "category_id"); f.brand_id = text(line, "brand_id"); f.shop_id = text(line, "shop_id");
        f.province_id = text(order, "province_id"); f.city_id = text(order, "city_id"); f.district_id = text(order, "district_id");
        f.currency = "CNY"; f.event_date = date; f.pay_date = payDate.isEmpty() ? null : payDate;
        f.amount_cent = amount; f.quantity = "REFUNDED".equals(type) ? 0 : integer(line, "quantity");
        return f;
    }
}
