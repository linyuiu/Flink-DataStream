package org.linyureal.mock.generator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.linyureal.common.Json;
import org.linyureal.model.cdc.Change;
import org.linyureal.mock.scenario.Scenario;
import org.linyureal.validation.TradeValidator;
import java.time.LocalDateTime;
import java.util.*;

/** Each inner list is ONE business DB transaction. Times are business times, not Kafka ingestion times. */
public final class TradeGenerator {
    private TradeGenerator() {}
    public static List<List<Change>> generate(String id,Scenario scenario,LocalDateTime created,Random random) {
        List<List<Change>> txs=new ArrayList<>();
        String shop=random.nextBoolean()?"S1":"S2";
        boolean sh=random.nextBoolean();
        ObjectNode order=row(id,id,created);
        order.put("user_id","U"+(random.nextInt(100)+1)); order.put("shop_id",shop);
        order.put("province_code",sh?"310000":"330000"); order.put("city_code",sh?"310100":"330100");
        order.put("district_code",sh?"310115":"330106"); order.put("currency_code","CNY"); order.put("status","CREATED");
        order.put("item_count",2); order.put("goods_cent",30000); order.put("freight_cent",1000); order.put("payable_cent",31000);
        ObjectNode a=item(id+"-I1",id,"P1","SKU1","C1","B1",shop,1,10000,created);
        ObjectNode b=item(id+"-I2",id,"P2","SKU2","C2","B2",shop,2,20000,created);
        txs.add(Arrays.asList(change("trade_order",order),change("trade_order_item",a),change("trade_order_item",b)));
        if(scenario==Scenario.CANCELLED) {
            ObjectNode cancelled=next(order,created.plusSeconds(30)); cancelled.put("status","CANCELLED");
            txs.add(Collections.singletonList(change("trade_order",cancelled))); return txs;
        }
        // PAYMENT_RETRY: failed attempt belongs in payment service logs, NOT success ledger.
        LocalDateTime paid=created.plusSeconds(scenario==Scenario.PAYMENT_RETRY?90:30);
        ObjectNode payment=row(id+"-PAY",id,paid);
        payment.put("status","SUCCEEDED"); payment.put("channel_transaction_id",id+"-CHANNEL");
        payment.put("amount_cent",31000); payment.put("pay_time",time(paid));
        ObjectNode pa=allocation(id+"-PA1",id,id+"-PAY",id+"-I1",10000,paid);
        ObjectNode pb=allocation(id+"-PA2",id,id+"-PAY",id+"-I2",20000,paid);
        ObjectNode paidOrder=next(order,paid); paidOrder.put("status","PAID");
        txs.add(Arrays.asList(change("trade_payment",payment),change("trade_payment_item",pa),
                change("trade_payment_item",pb),change("trade_order",paidOrder)));
        if(scenario==Scenario.PAID || scenario==Scenario.PAYMENT_RETRY) return txs;
        LocalDateTime refundTime=scenario==Scenario.CROSS_DAY_REFUND?paid.plusDays(1):paid.plusSeconds(60);
        addRefund(txs,id,"R1",new long[]{3000,0},refundTime,scenario!=Scenario.REFUND_FAILED);
        if(scenario==Scenario.MULTIPLE_REFUNDS) addRefund(txs,id,"R2",new long[]{2000,0},refundTime.plusSeconds(60),true);
        if(scenario==Scenario.FULL_REFUND) addRefund(txs,id,"R2",new long[]{7000,20000},refundTime.plusSeconds(60),true);
        return txs;
    }
    private static void addRefund(List<List<Change>> txs,String order,String suffix,long[] amounts,LocalDateTime t,boolean success) {
        String id=order+"-"+suffix;
        ObjectNode r=row(id,order,t);
        r.put("payment_id",order+"-PAY"); r.put("status","APPLIED");
        r.put("amount_cent",amounts[0]+amounts[1]); r.put("item_count",amounts[1]>0?2:1);
        r.put("refund_time",(String)null);
        List<Change> applied=new ArrayList<>(); applied.add(change("trade_refund",r));
        for(int i=0;i<amounts.length;i++) if(amounts[i]>0) {
            ObjectNode ri=row(id+"-RI"+(i+1),order,t); ri.put("refund_id",id);
            ri.put("order_item_id",order+"-I"+(i+1)); ri.put("amount_cent",amounts[i]);
            applied.add(change("trade_refund_item",ri));
        }
        txs.add(applied);
        ObjectNode result=next(r,t.plusSeconds(10)); result.put("status",success?"SUCCEEDED":"FAILED");
        if(success) result.put("refund_time",time(t.plusSeconds(10)));
        txs.add(Collections.singletonList(change("trade_refund",result)));
    }
    private static ObjectNode allocation(String id,String order,String pay,String item,long amount,LocalDateTime t) {
        ObjectNode r=row(id,order,t); r.put("payment_id",pay); r.put("order_item_id",item); r.put("amount_cent",amount); return r;
    }
    private static ObjectNode item(String id,String order,String product,String sku,String category,String brand,
                                   String shop,int quantity,long amount,LocalDateTime t) {
        ObjectNode r=row(id,order,t); r.put("product_id",product); r.put("sku_id",sku); r.put("category_id",category);
        r.put("brand_id",brand); r.put("shop_id",shop); r.put("quantity",quantity); r.put("payable_cent",amount); return r;
    }
    private static ObjectNode row(String id,String order,LocalDateTime t) {
        ObjectNode r=Json.MAPPER.createObjectNode(); r.put("id",id); r.put("order_id",order);
        r.put("version",1L); r.put("update_time",time(t)); return r;
    }
    private static ObjectNode next(ObjectNode r,LocalDateTime t) {
        ObjectNode n=r.deepCopy(); n.put("version",r.path("version").asLong()+1); n.put("update_time",time(t)); return n;
    }
    private static Change change(String table,ObjectNode row) { return new Change(table,row.path("version").asLong()==1?"c":"u",row); }
    private static String time(LocalDateTime t) { return t.format(TradeValidator.TIME); }
}
