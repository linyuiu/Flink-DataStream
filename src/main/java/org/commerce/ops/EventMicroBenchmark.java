package org.commerce.ops;

import org.commerce.common.Json;
import org.commerce.function.EventExpansion;
import org.commerce.mock.*;
import org.commerce.validation.EventContract;

import java.util.*;

/** CPU-only microbenchmark, explicitly NOT Kafka/CDC/RocksDB/Doris end-to-end capacity. */
public final class EventMicroBenchmark {
    public static void main(String[] args) {
        int iterations = args.length == 0 ? 100000 : Integer.parseInt(args[0]);
        if (iterations <= 0) throw new IllegalArgumentException("Iterations must be positive");
        List<String> inputs = new ArrayList<>();
        for (int i = 0; i < 1000; i++)
            for (TransactionPlan p :
                    BusinessGenerator.generate(
                            "bench-" + i,
                            Scenario.values()[i % Scenario.values().length],
                            System.currentTimeMillis() - 300000,
                            2740,
                            new Random(i))) if (p.event != null) inputs.add(Json.write(p.event));
        long checksum = 0;
        for (int i = 0; i < 10000; i++)
            checksum +=
                    EventExpansion.expand(
                                    EventContract.parse(inputs.get(i % inputs.size())), 32, 8, 4)
                            .size();
        long start = System.nanoTime(), expanded = 0;
        for (int i = 0; i < iterations; i++)
            expanded +=
                    EventExpansion.expand(
                                    EventContract.parse(inputs.get(i % inputs.size())), 32, 8, 4)
                            .size();
        double seconds = (System.nanoTime() - start) / 1e9;
        System.out.printf(
                Locale.ROOT,
                "CPU_ONLY events=%d expanded=%d seconds=%.3f events_per_second=%.1f checksum=%d%n",
                iterations,
                expanded,
                seconds,
                iterations / seconds,
                checksum);
    }
}
