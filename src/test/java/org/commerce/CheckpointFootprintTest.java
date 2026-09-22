package org.commerce;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.commerce.common.*;
import org.commerce.function.*;
import org.commerce.mock.*;
import org.commerce.model.MetricDelta;
import org.commerce.validation.EventContract;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.*;

/** 小样本 RocksDB 增量 checkpoint 观测，不把样本字节数冒充生产日百万订单容量。 */
class CheckpointFootprintTest {
    @Test
    @Timeout(60)
    void measureSeparateStateOwnersAndIncrementalCheckpointBytes() throws Exception {
        long now = System.currentTimeMillis();
        try (var facts =
                        new KeyedOneInputStreamOperatorTestHarness<>(
                                new KeyedProcessOperator<>(new AdmitAndDeduplicate(7)),
                                (String raw) -> Json.text(Json.object(raw), "eventId"),
                                Types.STRING);
                var unique =
                        new KeyedOneInputStreamOperatorTestHarness<>(
                                new KeyedProcessOperator<>(new ExactDistinctFunction(7)),
                                MetricDelta::distinctKey,
                                Types.STRING);
                var amounts =
                        new KeyedOneInputStreamOperatorTestHarness<>(
                                new KeyedProcessOperator<>(
                                        new ShardedAccumulator(93, 3000, MetricGroup.AMOUNT)),
                                MetricDelta::partitionKey,
                                Types.STRING);
                var counts =
                        new KeyedOneInputStreamOperatorTestHarness<>(
                                new KeyedProcessOperator<>(
                                        new ShardedAccumulator(9, 3000, MetricGroup.DISTINCT)),
                                MetricDelta::partitionKey,
                                Types.STRING)) {
            List<AbstractStreamOperatorTestHarness<?>> operators =
                    List.of(facts, unique, amounts, counts);
            for (var operator : operators) {
                var backend = new EmbeddedRocksDBStateBackend(true);
                backend.setPriorityQueueStateType(
                        EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB);
                operator.setStateBackend(backend);
                operator.open();
                operator.setProcessingTime(now);
                operator.setStateTtlProcessingTime(now);
            }

            int acceptedEvents = 0;
            for (int index = 0; index < 1000; index++) {
                Scenario scenario = index % 10 == 0 ? Scenario.MULTIPLE_REFUNDS : Scenario.PAID;
                for (TransactionPlan plan :
                        BusinessGenerator.generate(
                                "CK-" + index, scenario, now - 300000, 3000, new Random(index))) {
                    if (plan.event == null) {
                        continue;
                    }
                    plan.event.userId = "U-" + index;
                    facts.processElement(Json.write(plan.event), 0);
                    for (String accepted : facts.extractOutputValues()) {
                        acceptedEvents++;
                        for (MetricDelta delta :
                                EventExpansion.expand(EventContract.parse(accepted), 32, 8, 4)) {
                            if (delta.distinctKind == null) {
                                amounts.processElement(delta, 0);
                            } else {
                                unique.processElement(delta, 0);
                                for (MetricDelta first : unique.extractOutputValues()) {
                                    counts.processElement(first, 0);
                                }
                                unique.getOutput().clear();
                            }
                        }
                    }
                    facts.getOutput().clear();
                }
            }
            assertEquals(2200, acceptedEvents);
            assertEquals(0, facts.numProcessingTimeTimers(), "eventId 不为每个事件注册清理 timer");
            assertEquals(0, unique.numProcessingTimeTimers(), "精确去重使用 TTL，不为每个用户注册 timer");
            String[] names = {
                "fact-fingerprint", "exact-distinct", "amount-aggregate", "distinct-aggregate"
            };
            for (int index = 0; index < operators.size(); index++) {
                var operator = operators.get(index);
                operator.setProcessingTime(now + 3000);
                OperatorSubtaskState initial = operator.snapshot(1, now + 3000);
                operator.notifyOfCompletedCheckpoint(1);
                OperatorSubtaskState unchanged = operator.snapshot(2, now + 3001);
                assertTrue(initial.hasState());
                assertTrue(initial.getStateSize() > 0);
                assertTrue(unchanged.getCheckpointedSize() <= unchanged.getStateSize());
                System.out.printf(
                        Locale.ROOT,
                        "CHECKPOINT_SAMPLE component=%s orders=1000 events=2200 first_total_bytes=%d first_checkpointed_bytes=%d unchanged_total_bytes=%d unchanged_checkpointed_bytes=%d managed_keyed_bytes=%d raw_keyed_bytes=%d timers=%d%n",
                        names[index],
                        initial.getStateSize(),
                        initial.getCheckpointedSize(),
                        unchanged.getStateSize(),
                        unchanged.getCheckpointedSize(),
                        initial.getManagedKeyedState().getStateSize(),
                        initial.getRawKeyedState().getStateSize(),
                        operator.numProcessingTimeTimers());
            }
        }
    }
}
