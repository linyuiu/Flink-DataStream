package org.linyureal.realtime.mock;

import org.linyureal.realtime.common.Jsons;
import org.linyureal.realtime.config.PipelineConfig;
import org.linyureal.realtime.model.RowChange;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Finite, standalone MySQL producer. It does not publish to Kafka or execute schema DDL. */
public final class MySqlSimulator {
    public static void main(String[] args) throws Exception {
        PipelineConfig c = PipelineConfig.load(args);
        String runId = c.get("mock.run.id");
        if (!runId.matches("[A-Za-z0-9_-]{1,24}")) throw new IllegalArgumentException("Invalid run ID");
        long count = c.positive("mock.orders"), interval = 1_000_000_000L / c.positive("mock.orders.per.second");
        long delay = Long.parseLong(c.get("mock.transaction.delay.ms"));
        if (delay < 0) throw new IllegalArgumentException("Transaction delay cannot be negative");
        Random random = new Random(Long.parseLong(c.get("mock.seed")));
        LocalDateTime now = LocalDateTime.now(ZoneId.of("Asia/Shanghai"));
        LocalDateTime base = c.get("mock.base.time").equals("AUTO") ? now.minusMinutes(10) : LocalDateTime.parse(c.get("mock.base.time"), Jsons.TIME);
        if (base.plusMinutes(5).isAfter(now)) throw new IllegalArgumentException("Accelerated simulation base must be at least 5 minutes in the past");
        try (MySqlWriter writer = new MySqlWriter(c)) {
            writer.seedDimensions();
            for (long i = 0; i < count; i++) {
                long start = System.nanoTime(); Scenario scenario = Scenario.values()[(int) (i % Scenario.values().length)];
                LocalDateTime created = scenario == Scenario.CROSS_DAY_REFUND ? base.minusDays(1) : base;
                List<List<RowChange>> transactions = TransactionGenerator.generate(runId + "-" + i, scenario, created, random);
                int rows = 0;
                for (List<RowChange> transaction : transactions) {
                    writer.transaction(transaction); rows += transaction.size();
                    if (delay > 0) Thread.sleep(delay);
                }
                System.out.println("order=" + runId + "-" + i + " scenario=" + scenario + " transactions=" + transactions.size() + " row_changes=" + rows);
                long remaining = interval - (System.nanoTime() - start);
                if (remaining > 0) TimeUnit.NANOSECONDS.sleep(remaining);
            }
            if (Boolean.parseBoolean(c.get("mock.rename.dimension"))) writer.renameDemoShop();
        }
    }
}
