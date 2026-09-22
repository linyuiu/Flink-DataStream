package org.commerce;

import static org.junit.jupiter.api.Assertions.*;

import org.commerce.common.Contract;
import org.commerce.common.Json;
import org.commerce.common.Metric;
import org.commerce.mock.BusinessGenerator;
import org.commerce.mock.Scenario;
import org.commerce.mock.TransactionPlan;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Random;

/** 可读性重构不能悄悄改业务结果或持久化指标布局。 */
class ReadabilityRegressionTest {
    @Test
    void namedMetricsPreserveExistingCheckpointAndDorisLayout() {
        String[] legacyColumns = {
            "created_orders", "cancelled_orders", "paid_orders", "paid_users",
            "refund_orders", "refund_requests", "paid_quantity", "created_goods_cent",
            "paid_goods_cent", "refund_goods_cent", "net_paid_goods_cent"
        };
        assertArrayEquals(legacyColumns, Contract.METRICS);
        assertEquals(legacyColumns.length, Metric.values().length);
        for (Metric metric : Metric.values()) {
            assertEquals(legacyColumns[metric.index()], metric.columnName());
        }
        assertEquals(
                legacyColumns.length,
                java.util.Arrays.stream(Metric.values()).map(Metric::index).distinct().count());
    }

    @Test
    void generatedTransactionsAndEventsMatchPreRefactorBaseline() throws Exception {
        // 重构前记录的九场景 × 二十 seed，包含每次事务的行顺序、字段、版本和 Outbox 内容。
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (Scenario scenario : Scenario.values()) {
            for (int seed = 0; seed < 20; seed++) {
                for (TransactionPlan plan :
                        BusinessGenerator.generate(
                                "REFERENCE-" + seed,
                                scenario,
                                1789185600000L,
                                2740,
                                new Random(seed))) {
                    for (TransactionPlan.Row row : plan.rows) {
                        digest.update(
                                (row.table + "|" + row.update + "|" + row.value + "\n")
                                        .getBytes(StandardCharsets.UTF_8));
                    }
                    digest.update((Json.write(plan.event) + "\n").getBytes(StandardCharsets.UTF_8));
                }
            }
        }
        StringBuilder actual = new StringBuilder();
        for (byte value : digest.digest()) {
            actual.append(String.format("%02x", value & 0xff));
        }
        assertEquals(
                "9beb5ac903805a68e76c7b3e65547976a8ec7db1f78912dc2777d6b81fcdc66c",
                actual.toString(),
                "整理代码不应改变模拟交易；只有明确变更业务契约时才能审核更新此基线");
    }
}
