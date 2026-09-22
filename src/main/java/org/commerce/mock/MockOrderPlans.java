package org.commerce.mock;

import org.commerce.config.CommerceConfig;

import java.util.List;
import java.util.Random;

/** 两种模拟入口共用同一订单抽样规则与交易计划，避免数据口径随写入目标变化。 */
final class MockOrderPlans {
    private final String runPrefix;
    private final String scenarioMode;
    private final double paymentRate;
    private final long averageGoodsCent;
    private final long randomSeed;
    private final long fixedStartMillis;
    private final long ordersPerSecond;

    MockOrderPlans(CommerceConfig config) {
        config.validateMock();
        runPrefix = config.get("mock.run.id");
        if (!runPrefix.matches("[A-Za-z0-9_-]{1,20}")) {
            throw new IllegalArgumentException("Invalid run prefix");
        }
        scenarioMode = config.get("mock.scenario");
        if (!scenarioMode.equals("PROFILE") && !scenarioMode.equals("MATRIX")) {
            Scenario.valueOf(scenarioMode);
        }
        paymentRate = Double.parseDouble(config.get("business.payment.rate"));
        averageGoodsCent = config.positive("business.average.paid.cent");
        randomSeed = Long.parseLong(config.get("mock.seed"));
        fixedStartMillis = config.positive("mock.start.time.ms", 0);
        ordersPerSecond = config.positive("mock.orders.per.second");
    }

    List<TransactionPlan> generate(long orderIndex, long nowMillis) {
        Random random = new Random(randomSeed + orderIndex);
        Scenario scenario = chooseScenario(orderIndex, random);
        // 这里的平均金额是订单金额；每个订单仍按原规则在 60%~140% 间波动。
        long goodsCent =
                Math.max(200, Math.round(averageGoodsCent * (.6 + .8 * random.nextDouble())));
        // 固定业务时钟时，时间只由订单序号决定，不受两个任务的启动时刻和线程调度影响。
        long createdAt =
                fixedStartMillis == 0
                        ? nowMillis - 300_000
                        : Math.addExact(
                                fixedStartMillis,
                                Math.multiplyExact(orderIndex, 1_000L) / ordersPerSecond);
        if (scenario == Scenario.OLD_ORDER_REFUND) {
            createdAt -= 14L * 86_400_000;
        }
        return BusinessGenerator.generate(
                runPrefix + "-" + orderIndex, scenario, createdAt, goodsCent, random);
    }

    private Scenario chooseScenario(long orderIndex, Random random) {
        if (scenarioMode.equals("MATRIX")) {
            return Scenario.values()[(int) (orderIndex % Scenario.values().length)];
        }
        if (!scenarioMode.equals("PROFILE")) {
            return Scenario.valueOf(scenarioMode);
        }
        if (random.nextDouble() >= paymentRate) {
            return random.nextDouble() < .8 ? Scenario.CANCELLED : Scenario.PAYMENT_FAILED;
        }

        // 以下是已支付订单中的累计概率区间，不是所有创建订单的概率。
        double sample = random.nextDouble();
        if (sample < .05) {
            return Scenario.PARTIAL_REFUND;
        }
        if (sample < .10) {
            return Scenario.MULTIPLE_REFUNDS;
        }
        if (sample < .15) {
            return Scenario.FULL_GOODS_REFUND;
        }
        if (sample < .18) {
            return Scenario.REFUND_FAILED;
        }
        if (sample < .28) {
            return Scenario.RETRY_PAID;
        }
        return Scenario.PAID;
    }
}
