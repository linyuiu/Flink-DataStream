package org.linyu.marketing.function;

import org.linyu.marketing.model.CartEvent;
import org.linyu.marketing.model.PayEvent;
import org.linyu.marketing.model.CouponCandidate;
import org.linyu.marketing.config.RuleConfig;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 用户 + SKU 维度的营销候选规则。
 *
 * 该函数同时接收加购流和支付流：加购会注册未支付定时器，支付会取消同一用户同一 SKU 的未支付状态。
 */
public class UserSkuRuleFunction
        extends KeyedCoProcessFunction<String, CartEvent, PayEvent, CouponCandidate> {

    private ValueState<CartHoldState> unpaidCartState;
    private ListState<Long> recentCartTimeState;
    private MapState<String, Boolean> eventDedupState;

    public static class CartHoldState {
        public long userId;
        public long skuId;
        public long cartEventTime;
        public long timerTime;

        public CartHoldState() {}

        public CartHoldState(long userId, long skuId, long cartEventTime, long timerTime) {
            this.userId = userId;
            this.skuId = skuId;
            this.cartEventTime = cartEventTime;
            this.timerTime = timerTime;
        }
    }

    @Override
    public void open(Configuration parameters) {
        // 加购相关状态只需要覆盖规则窗口和定时器等待时间，过期后直接丢弃即可。
        StateTtlConfig twoHourTtl = StateTtlConfig
                .newBuilder(Time.hours(2))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor<CartHoldState> cartDesc =
                new ValueStateDescriptor<>("unpaid-cart-state", CartHoldState.class);
        cartDesc.enableTimeToLive(twoHourTtl);
        unpaidCartState = getRuntimeContext().getState(cartDesc);

        ListStateDescriptor<Long> listDesc =
                new ListStateDescriptor<>("recent-cart-time-state", Long.class);
        listDesc.enableTimeToLive(twoHourTtl);
        recentCartTimeState = getRuntimeContext().getListState(listDesc);

        // Debezium/Kafka 在故障恢复后可能重放事件；去重状态保留更久，避免重复发候选券。
        StateTtlConfig dedupTtl = StateTtlConfig
                .newBuilder(Time.days(2))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        MapStateDescriptor<String, Boolean> dedupDesc =
                new MapStateDescriptor<>("event-dedup-state", String.class, Boolean.class);
        dedupDesc.enableTimeToLive(dedupTtl);
        eventDedupState = getRuntimeContext().getMapState(dedupDesc);
    }

    @Override
    public void processElement1(
            CartEvent cart,
            Context ctx,
            Collector<CouponCandidate> out
    ) throws Exception {

        if (isDuplicate(cart.eventId)) {
            return;
        }
        eventDedupState.put(cart.eventId, true);

        // 规则 A：加购 30 分钟未支付。使用 processing-time，强调“系统看到事件后多久触达”。
        CartHoldState oldState = unpaidCartState.value();
        if (oldState != null) {
            // 同一用户同一 SKU 再次加购时，以最新一次加购重新计算等待时间。
            ctx.timerService().deleteProcessingTimeTimer(oldState.timerTime);
        }

        long timerTime =
                ctx.timerService().currentProcessingTime()
                        + RuleConfig.ABANDON_CART_DELAY_MS;

        unpaidCartState.update(
                new CartHoldState(cart.userId, cart.skuId, cart.eventTime, timerTime)
        );

        ctx.timerService().registerProcessingTimeTimer(timerTime);

        // 规则 B：保留 1 小时窗口内的加购时间，用数量判断是否重复加购同一 SKU。
        List<Long> recentTimes = new ArrayList<>();

        for (Long t : recentCartTimeState.get()) {
            if (t >= cart.eventTime - RuleConfig.REPEAT_CART_WINDOW_MS) {
                recentTimes.add(t);
            }
        }

        recentTimes.add(cart.eventTime);
        recentCartTimeState.update(recentTimes);

        if (recentTimes.size() >= RuleConfig.REPEAT_CART_THRESHOLD) {
            CouponCandidate candidate = new CouponCandidate();
            candidate.userId = cart.userId;
            candidate.skuId = cart.skuId;
            candidate.ruleCode = "REPEAT_CART_1H";
            candidate.eventTime = cart.eventTime;
            candidate.reason = "用户 1 小时内多次加购同一 SKU";
            candidate.sourceEventId = cart.eventId;

            out.collect(candidate);
        }
    }

    @Override
    public void processElement2(
            PayEvent pay,
            Context ctx,
            Collector<CouponCandidate> out
    ) throws Exception {

        if (isDuplicate(pay.eventId)) {
            return;
        }
        eventDedupState.put(pay.eventId, true);

        // 用户支付了这个 SKU，说明“加购未支付”条件已失效，需要取消等待中的定时器。
        CartHoldState state = unpaidCartState.value();
        if (state != null) {
            ctx.timerService().deleteProcessingTimeTimer(state.timerTime);
            unpaidCartState.clear();
        }

        // 支付后清理重复加购窗口，避免购买前的旧加购继续贡献后续发券判断。
        recentCartTimeState.clear();
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<CouponCandidate> out
    ) throws Exception {

        CartHoldState state = unpaidCartState.value();
        if (state == null) {
            return;
        }

        if (timestamp != state.timerTime) {
            // 防御旧定时器：状态已被新加购覆盖时，旧 timer 触发不能再产出候选事件。
            return;
        }

        CouponCandidate candidate = new CouponCandidate();
        candidate.userId = state.userId;
        candidate.skuId = state.skuId;
        candidate.ruleCode = "ABANDON_CART_30MIN";
        candidate.eventTime = state.cartEventTime;
        candidate.triggerTime = timestamp;
        candidate.reason = "用户加购 30 分钟未支付";

        out.collect(candidate);

        unpaidCartState.clear();
    }

    private boolean isDuplicate(String eventId) throws Exception {
        if (eventId == null || eventId.isEmpty()) {
            return false;
        }
        return eventDedupState.contains(eventId);
    }
}
