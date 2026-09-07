package org.linyureal.function.trade;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.linyureal.common.Json;
import org.linyureal.model.cdc.Change;
import org.linyureal.model.metric.ItemFact;
import org.linyureal.model.trade.TradeState;
import org.linyureal.validation.TradeValidator;
import java.util.*;

/** Builds facts only when transaction headers and all declared details are present.
 * Caller commits candidate state only after accept succeeds. */
public final class TradeAssembler {
    private TradeAssembler() {}
    public static List<ItemFact> accept(TradeState state, Change c) throws Exception {
        TradeValidator.validate(c);
        // Normalize Jackson IntNode/LongNode representation before semantic equality checks.
        c = new Change(c.table,c.op,(ObjectNode)Json.MAPPER.readTree(Json.write(c.after)));
        for(String row:state.rows.values()) {
            if(!Json.text(Json.MAPPER.readTree(row),"order_id").equals(c.orderId()))
                throw new IllegalArgumentException("Mixed order keys");
        }
        if(c.table.equals("trade_order") && !Json.text(c.after,"id").equals(c.orderId()))
            throw new IllegalArgumentException("Order id/order_id mismatch");
        String key = c.table + ":" + Json.text(c.after,"id");
        String prior = state.rows.get(key);
        if (prior != null) {
            ObjectNode old = (ObjectNode)Json.MAPPER.readTree(prior);
            long v = Json.number(c.after,"version"), oldV = Json.number(old,"version");
            if(v < oldV) return Collections.emptyList();
            if(v == oldV) {
                if(!old.equals(c.after)) throw new IllegalArgumentException("Same version has conflicting content: " + key);
                return Collections.emptyList();
            }
            // Financial ledger rows and item allocations are immutable in this bounded business model.
            if(!c.table.equals("trade_order") && !c.table.equals("trade_refund")) {
                ObjectNode a=old.deepCopy(), b=c.after.deepCopy();
                a.remove(Arrays.asList("version","update_time")); b.remove(Arrays.asList("version","update_time"));
                if(!a.equals(b)) throw new IllegalArgumentException("Immutable ledger/item changed: " + key);
            }
            if(c.table.equals("trade_refund") && "SUCCEEDED".equals(Json.text(old,"status"))
                    && !"SUCCEEDED".equals(Json.text(c.after,"status")))
                throw new IllegalArgumentException("Succeeded refund cannot regress");
        }
        state.rows.put(key, Json.write(c.after));
        Map<String,ObjectNode> orders=rows(state,"trade_order");
        if(orders.size()>1) throw new IllegalArgumentException("Multiple orders in one key");
        if(orders.isEmpty()) return Collections.emptyList();
        ObjectNode order=orders.values().iterator().next();
        Map<String,ObjectNode> items=rows(state,"trade_order_item");
        Map<String,ObjectNode> payments=rows(state,"trade_payment");
        Map<String,ObjectNode> allocations=rows(state,"trade_payment_item");
        Map<String,ObjectNode> refunds=rows(state,"trade_refund");
        Map<String,ObjectNode> refundItems=rows(state,"trade_refund_item");
        long expected=Json.number(order,"item_count");
        if(items.size()>expected || payments.size()>1 || allocations.size()>expected)
            throw new IllegalArgumentException("Unexpected item/payment count");
        if(items.size()!=expected || payments.isEmpty() || allocations.size()!=expected)
            return Collections.emptyList();
        if("CANCELLED".equals(Json.text(order,"status"))) throw new IllegalArgumentException("Cancelled unpaid order has payment");
        ObjectNode payment=payments.values().iterator().next();
        String paymentId=Json.text(payment,"id");
        if(Json.number(payment,"amount_cent") != Json.number(order,"payable_cent"))
            throw new IllegalArgumentException("Payment/order amount mismatch");
        long goods=0;
        for(ObjectNode item:items.values()) {
            if(!Json.text(item,"shop_id").equals(Json.text(order,"shop_id"))) throw new IllegalArgumentException("Cross-shop item");
            goods=Math.addExact(goods,Json.number(item,"payable_cent"));
        }
        if(goods!=Json.number(order,"goods_cent")) throw new IllegalArgumentException("Item/order goods mismatch");
        Set<String> allocated=new HashSet<>();
        List<ItemFact> candidates=new ArrayList<>();
        for(ObjectNode allocation:allocations.values()) {
            String itemId=Json.text(allocation,"order_item_id");
            ObjectNode item=items.get(itemId);
            if(item==null || !allocated.add(itemId) || !paymentId.equals(Json.text(allocation,"payment_id")))
                throw new IllegalArgumentException("Invalid payment allocation reference");
            if(Json.number(allocation,"amount_cent")!=Json.number(item,"payable_cent"))
                throw new IllegalArgumentException("Payment allocation amount mismatch");
            candidates.add(fact("PAYMENT",Json.text(allocation,"id"),order,item,payment,null,
                    Json.number(allocation,"amount_cent")));
        }
        Map<String,Long> refunded=new HashMap<>();
        for(ObjectNode refund:refunds.values()) {
            if(!paymentId.equals(Json.text(refund,"payment_id"))) throw new IllegalArgumentException("Refund payment mismatch");
            String refundId=Json.text(refund,"id");
            List<ObjectNode> details=new ArrayList<>();
            for(ObjectNode ri:refundItems.values()) if(refundId.equals(Json.text(ri,"refund_id"))) details.add(ri);
            long count=Json.number(refund,"item_count");
            if(details.size()>count) throw new IllegalArgumentException("Too many refund details");
            if(!"SUCCEEDED".equals(Json.text(refund,"status")) || details.size()!=count) continue;
            if(java.time.LocalDateTime.parse(Json.text(refund,"refund_time"),TradeValidator.TIME)
                    .isBefore(java.time.LocalDateTime.parse(Json.text(payment,"pay_time"),TradeValidator.TIME)))
                throw new IllegalArgumentException("Refund success precedes payment");
            long total=0;
            Set<String> refundItemIds=new HashSet<>();
            for(ObjectNode ri:details) {
                String itemId=Json.text(ri,"order_item_id");
                if(!refundItemIds.add(itemId)) throw new IllegalArgumentException("Repeated item within refund");
                ObjectNode item=items.get(itemId);
                if(item==null) throw new IllegalArgumentException("Unknown refund order item");
                long amount=Json.number(ri,"amount_cent");
                total=Math.addExact(total,amount);
                long cumulative=Math.addExact(refunded.getOrDefault(itemId,0L),amount);
                if(cumulative>Json.number(item,"payable_cent")) throw new IllegalArgumentException("Over-refund: "+itemId);
                refunded.put(itemId,cumulative);
                candidates.add(fact("REFUND",Json.text(ri,"id"),order,item,payment,refund,amount));
            }
            if(total!=Json.number(refund,"amount_cent")) throw new IllegalArgumentException("Refund header/detail mismatch");
        }
        List<ItemFact> output=new ArrayList<>();
        // Validate all projections before mutating emitted markers.
        for(ItemFact f:candidates) {
            String old=state.emitted.get(f.fact_id);
            if(old!=null && !old.equals(Json.write(f)))
                throw new IllegalArgumentException("Published fact changed; requires audited correction: "+f.fact_id);
        }
        for(ItemFact f:candidates) if(!state.emitted.containsKey(f.fact_id)) {
            state.emitted.put(f.fact_id,Json.write(f)); output.add(f);
        }
        return output;
    }
    private static Map<String,ObjectNode> rows(TradeState state,String table) throws Exception {
        Map<String,ObjectNode> result=new LinkedHashMap<>();
        for(Map.Entry<String,String> e:state.rows.entrySet()) if(e.getKey().startsWith(table+":")) {
            ObjectNode row=(ObjectNode)Json.MAPPER.readTree(e.getValue());
            result.put(Json.text(row,"id"),row);
        }
        return result;
    }
    private static ItemFact fact(String type,String id,ObjectNode order,ObjectNode item,
                                 ObjectNode payment,ObjectNode refund,long amount) {
        ItemFact f=new ItemFact();
        f.fact_id=type+":"+id; f.fact_type=type; f.order_id=Json.text(order,"order_id");
        f.order_item_id=Json.text(item,"id"); f.product_id=Json.text(item,"product_id");
        f.sku_id=Json.text(item,"sku_id"); f.category_id=Json.text(item,"category_id");
        f.brand_id=Json.text(item,"brand_id"); f.shop_id=Json.text(item,"shop_id");
        f.province_code=Json.text(order,"province_code"); f.city_code=Json.text(order,"city_code");
        f.district_code=Json.text(order,"district_code"); f.currency_code="CNY";
        f.pay_date=TradeValidator.date(payment,"pay_time");
        f.event_date=refund==null ? f.pay_date : TradeValidator.date(refund,"refund_time");
        f.amount_cent=amount; return f;
    }
}
