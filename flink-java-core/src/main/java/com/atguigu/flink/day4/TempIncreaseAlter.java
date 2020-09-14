package com.atguigu.flink.day4;

import com.atguigu.flink.day2.SensorReading;
import com.atguigu.flink.day2.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/14 16:38
 */
public class TempIncreaseAlter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new SensorSource())
                .keyBy(r -> r.getId())
                .process(new TempIncrease())
                .print();

        env.execute();
    }

    public static class TempIncrease extends KeyedProcessFunction<String, SensorReading, String> {

        ValueState<Double> lastTemp;
        ValueState<Long> timer;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("last-temp", Types.DOUBLE)
            );
            timer = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timer", Types.LONG)
            );
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            double preTemp = lastTemp.value() == null ? 0 : lastTemp.value();
            long ts = timer.value() == null ? 0 : timer.value();

            Double tamperature = value.getTamperature();
            lastTemp.update(tamperature);

            if (preTemp == 0 || tamperature < preTemp) {
                ctx.timerService().deleteProcessingTimeTimer(ts);
                timer.clear();
            } else if (tamperature >= preTemp && ts == 0) {
                long oneSecondLater = ctx.timerService().currentProcessingTime() + 1000;
                ctx.timerService().registerProcessingTimeTimer(oneSecondLater);
                timer.update(oneSecondLater);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            timer.clear();
            out.collect("key:" + ctx.getCurrentKey() + ",报警了 time=" + timestamp);
        }
    }
}
