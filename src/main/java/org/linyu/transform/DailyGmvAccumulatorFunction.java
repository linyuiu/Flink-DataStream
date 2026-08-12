package org.linyu.transform;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


import org.linyu.config.BusinessTime;
import org.linyu.config.EnumV;
import org.linyu.map.DailyGmvRealTime;
import org.linyu.map.GmvDeltaRealTime;


import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 按照日期累计GMV变化量。
 *
 * Key：
 * bizDate
 *
 * State：
 * 这一天当前GMV总额。
 *
 * Timer：
 * 每5秒最多输出一次当前结果。
 */
public class DailyGmvAccumulatorFunction
        extends KeyedProcessFunction<
        String,
        GmvDeltaRealTime,
        DailyGmvRealTime> {
    private transient ValueState<BigDecimal> totalGmvState;
    private transient ValueState<Long> nextTimerState;
    private transient ValueState<Boolean> dirtyState;




    @Override
    public void processElement(GmvDeltaRealTime gmvDetailRealTIme,
                               Context context,
                               Collector<DailyGmvRealTime> collector) throws Exception {
        BigDecimal previousTotal = totalGmvState.value();

        if (previousTotal == null){
            previousTotal = BigDecimal.ZERO;
        }
        BigDecimal currenTotal =
                previousTotal.add(
                        gmvDetailRealTIme.deltaAmount
                );

        /*
         * 更新当天累计GMV。
         */
        totalGmvState.update(
                currenTotal
        );
        /*
         * 标记当前GMV自上次输出后发生过变化。
         */
        dirtyState.update(true);

        /*
         * 当前日期还没有注册输出定时器时，
         * 注册下一个5秒边界。
         */
        Long currentTimer = nextTimerState.value();

        if (currentTimer == null) {
            long now = context.timerService()
                    .currentProcessingTime();

            long nextTimer = now - now % EnumV.OUTPUT_INTERVAL_MS
                    + EnumV.OUTPUT_INTERVAL_MS;

            context.timerService()
                    .registerProcessingTimeTimer(
                            nextTimer
                    );

            nextTimerState.update(nextTimer);
        }



    }

    @Override
    public void open(OpenContext openContext) throws Exception {

        totalGmvState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "daily-total-gmv",
                        BigDecimal.class
                )
        );

        nextTimerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "daily-next-output-timer",
                        Long.class
                )
        );

        dirtyState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "daily-gmv-dirty",
                        Boolean.class
                )
        );

    }
    @Override
    public void onTimer(long timestamp,
                        OnTimerContext ctx,
                        Collector<DailyGmvRealTime> out) throws Exception {

        Boolean dirty =
                dirtyState.value();

        BigDecimal currentTotal =
                totalGmvState.value();

        /*
         * 只有 gmv 发生过变化时才输出
         * */
        if (Boolean.TRUE.equals(dirty) && currentTotal != null) {

            out.collect(new DailyGmvRealTime(
                            ctx.getCurrentKey(),
                            currentTotal,
                            currentTime()
                    )
            );
        }
        nextTimerState.clear();
        dirtyState.clear();



    }
    private static String currentTime() {
        return LocalDateTime
                .now(EnumV.BUSINESS_ZONE)
                .format(EnumV.DATE_TIME_FORMATTER);
    }

}
