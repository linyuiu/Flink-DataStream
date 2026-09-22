package org.commerce.function;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.commerce.common.Json;
import org.commerce.model.TradeEvent;
import org.commerce.validation.EventContract;

/** DWD消费边界：复核契约和在线时间范围，所有指标组使用同一准入口径。 */
public class TradeFactParseFunction extends ProcessFunction<String, String> {
    private static final long MAX_FUTURE_SKEW_MILLIS = 5 * 60_000L;
    private final long eventDays;
    private transient Counter invalidFacts;
    private transient Counter expiredFacts;
    private transient Counter acceptedFacts;

    /** 审计保留合法历史事实，不使用指标的在线日期准入限制。 */
    public TradeFactParseFunction() {
        this.eventDays = 0;
    }

    /** 金额和去重Job在展开维度前检查事件日期，避免积压后使用不同准入标准。 */
    public TradeFactParseFunction(long eventDays) {
        if (eventDays <= 0) {
            throw new IllegalArgumentException("Event admission days must be positive");
        }
        this.eventDays = eventDays;
    }

    @Override
    public void open(OpenContext context) {
        invalidFacts = getRuntimeContext().getMetricGroup().counter("invalid_facts");
        expiredFacts = getRuntimeContext().getMetricGroup().counter("expired_facts");
        acceptedFacts = getRuntimeContext().getMetricGroup().counter("accepted_facts");
    }

    @Override
    public void processElement(String raw, Context context, Collector<String> output) {
        TradeEvent event;
        try {
            event = EventContract.parse(raw);
        } catch (RuntimeException exception) {
            rejectInvalid(raw, exception.getMessage(), context);
            return;
        }

        if (eventDays > 0) {
            long now = context.timerService().currentProcessingTime();
            if (event.occurredAt > now + MAX_FUTURE_SKEW_MILLIS) {
                rejectInvalid(raw, "Business event is over 5 minutes in the future", context);
                return;
            }
            if (!Horizons.open(Horizons.date(event.occurredAt), now, eventDays)) {
                expiredFacts.inc();
                context.output(
                        Outputs.REPAIR,
                        Outputs.record(
                                "fact-horizon",
                                raw,
                                "DWD event exceeded online admission; reconcile and backfill"));
                return;
            }
        }

        acceptedFacts.inc();
        output.collect(Json.write(event));
    }

    private void rejectInvalid(String raw, String reason, Context context) {
        invalidFacts.inc();
        context.output(Outputs.QUALITY, Outputs.record("trade-fact-contract", raw, reason));
    }
}
