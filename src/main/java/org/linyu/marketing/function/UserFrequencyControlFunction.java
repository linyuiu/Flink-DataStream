package org.linyu.marketing.function;

import org.linyu.marketing.config.RuleConfig;
import org.linyu.marketing.model.CouponCandidate;
import org.linyu.marketing.model.CouponTrigger;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * 用户维度的最终触达频控。
 *
 * 上游可能产生多个候选规则，这里统一拦截过于频繁或疑似薅券的触达。
 */
public class UserFrequencyControlFunction
        extends KeyedProcessFunction<Long, CouponCandidate, CouponTrigger> {

    private MapState<String, Long> lastUserRulePushTimeState;
    private MapState<String, Long> lastUserSkuRulePushTimeState;
    private ListState<Long> userDailyPushTimeState;
    private ListState<Long> abandonCartCandidateTimeState;

    @Override
    public void open(Configuration parameters) {
        // 频控窗口最长 30 天，TTL 与最长规则保持一致，避免状态无限增长。
        StateTtlConfig ttlThirtyDays = StateTtlConfig
                .newBuilder(Time.days(30))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        MapStateDescriptor<String, Long> userRuleDesc =
                new MapStateDescriptor<>(
                        "last-user-rule-push-time",
                        String.class,
                        Long.class
                );
        userRuleDesc.enableTimeToLive(ttlThirtyDays);
        lastUserRulePushTimeState = getRuntimeContext().getMapState(userRuleDesc);

        MapStateDescriptor<String, Long> userSkuRuleDesc =
                new MapStateDescriptor<>(
                        "last-user-sku-rule-push-time",
                        String.class,
                        Long.class
                );
        userSkuRuleDesc.enableTimeToLive(ttlThirtyDays);
        lastUserSkuRulePushTimeState = getRuntimeContext().getMapState(userSkuRuleDesc);

        ListStateDescriptor<Long> dailyPushDesc =
                new ListStateDescriptor<>("user-daily-push-time", Long.class);
        dailyPushDesc.enableTimeToLive(ttlThirtyDays);
        userDailyPushTimeState = getRuntimeContext().getListState(dailyPushDesc);

        ListStateDescriptor<Long> abandonCartRiskDesc =
                new ListStateDescriptor<>("abandon-cart-candidate-time", Long.class);
        abandonCartRiskDesc.enableTimeToLive(ttlThirtyDays);
        abandonCartCandidateTimeState = getRuntimeContext().getListState(abandonCartRiskDesc);
    }

    @Override
    public void processElement(
            CouponCandidate candidate,
            Context ctx,
            Collector<CouponTrigger> out
    ) throws Exception {

        long now = ctx.timerService().currentProcessingTime();

        // 先记录候选触发，再做风控判断：用户频繁触发但被频控拦截，也属于风险信号。
        if ("ABANDON_CART_30MIN".equals(candidate.ruleCode)) {
            updateAbandonCartRisk(candidate, now);
        }

        // 规则 1：同一用户同一规则 7 天只能触达一次。
        Long lastUserRulePushTime =
                lastUserRulePushTimeState.get(candidate.ruleCode);

        if (lastUserRulePushTime != null
                && now - lastUserRulePushTime < RuleConfig.USER_RULE_INTERVAL_MS) {
            return;
        }

        // 规则 2：同一用户同一 SKU 同一规则 30 天只能触达一次。
        String userSkuRuleKey = candidate.ruleCode + "_" + candidate.skuId;

        Long lastUserSkuRulePushTime =
                lastUserSkuRulePushTimeState.get(userSkuRuleKey);

        if (lastUserSkuRulePushTime != null
                && now - lastUserSkuRulePushTime < RuleConfig.USER_SKU_RULE_INTERVAL_MS) {
            return;
        }

        // 规则 3：同一用户一天最多触达 3 次。
        List<Long> todayPushTimes = getTodayPushTimes(now);

        if (todayPushTimes.size() >= RuleConfig.USER_DAILY_MAX_PUSH) {
            return;
        }

        // 规则 4：判断是否有明显薅券行为。
        if (isPotentialCouponAbuser(now)) {
            return;
        }

        // 使用按周稳定的 triggerId，便于 Kafka/Doris 下游按唯一键做幂等写入。
        String triggerId = buildWeeklyTriggerId(
                candidate.userId,
                candidate.skuId,
                candidate.ruleCode,
                now
        );

        CouponTrigger trigger = new CouponTrigger();
        trigger.triggerId = triggerId;
        trigger.userId = candidate.userId;
        trigger.skuId = candidate.skuId;
        trigger.ruleCode = candidate.ruleCode;
        trigger.triggerTime = now;
        trigger.reason = candidate.reason;

        out.collect(trigger);

        // 只有真正输出触达后才更新频控状态。
        lastUserRulePushTimeState.put(candidate.ruleCode, now);
        lastUserSkuRulePushTimeState.put(userSkuRuleKey, now);

        todayPushTimes.add(now);
        userDailyPushTimeState.update(todayPushTimes);
    }

    private void updateAbandonCartRisk(
            CouponCandidate candidate,
            long now
    ) throws Exception {

        List<Long> recent = new ArrayList<>();

        for (Long t : abandonCartCandidateTimeState.get()) {
            if (now - t <= 7L * 24 * 60 * 60 * 1000L) {
                recent.add(t);
            }
        }

        recent.add(now);
        abandonCartCandidateTimeState.update(recent);
    }

    private boolean isPotentialCouponAbuser(long now) throws Exception {
        int count = 0;

        for (Long t : abandonCartCandidateTimeState.get()) {
            if (now - t <= 7L * 24 * 60 * 60 * 1000L) {
                count++;
            }
        }

        return count >= RuleConfig.ABANDON_CART_RISK_THRESHOLD_7D;
    }

    private List<Long> getTodayPushTimes(long now) throws Exception {
        List<Long> result = new ArrayList<>();

        // “一天”按业务所在地自然日计算，而不是 UTC 日期。
        LocalDate today = Instant.ofEpochMilli(now)
                .atZone(ZoneId.of("Asia/Shanghai"))
                .toLocalDate();

        for (Long t : userDailyPushTimeState.get()) {
            LocalDate d = Instant.ofEpochMilli(t)
                    .atZone(ZoneId.of("Asia/Shanghai"))
                    .toLocalDate();

            if (today.equals(d)) {
                result.add(t);
            }
        }

        return result;
    }

    private String buildWeeklyTriggerId(
            long userId,
            long skuId,
            String ruleCode,
            long now
    ) {
        LocalDate date = Instant.ofEpochMilli(now)
                .atZone(ZoneId.of("Asia/Shanghai"))
                .toLocalDate();

        int year = date.getYear();
        int week = date.get(java.time.temporal.WeekFields.ISO.weekOfWeekBasedYear());

        return userId + "_" + skuId + "_" + ruleCode + "_" + year + "W" + week;
    }
}
