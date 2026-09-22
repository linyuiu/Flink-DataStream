package org.commerce.model;

import java.util.ArrayList;
import java.util.List;

/** 不可变业务事实的传输模型，schema v1，金额单位为人民币分。 对象本身是可写 POJO，便于 JSON/Flink 序列化；事件一旦发布，业务上禁止修改其内容。 */
public class TradeEvent {
    public int schemaVersion = 1;
    public String eventId;
    public String eventType;

    /** 创建/取消对应订单 ID；支付对应成功流水 ID；退款对应本次退款请求 ID。 */
    public String businessId;

    public String orderId;
    public String checkoutId;
    public String userId;
    public String shopId;
    public String provinceId;
    public String cityId;
    public String districtId;
    public String currency;

    /** 本次业务事实发生时间，epoch 毫秒。 */
    public long occurredAt;

    /** 原支付成功时间；退款必须保留它，才能回扣原支付日净 GMV。 */
    public long paidAt;

    public long goodsCent;
    public List<Line> lines = new ArrayList<>();

    public TradeEvent() {}

    public static class Line {
        public String orderLineId;
        public String productId;
        public String skuId;
        public String categoryId;
        public String brandId;
        public long amountCent;

        /** 退款事件为 0：退了多少钱不能推断退了多少件货。 */
        public long quantity;

        public Line() {}
    }
}
