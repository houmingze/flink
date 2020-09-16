package com.atguigu.flink.day6;

import com.atguigu.flink.day2.SensorReading;
import com.atguigu.flink.day2.SensorSource;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/16 15:48
 */
public class ListStateExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new SensorSource())
                .filter(r -> r.getId().equals("Sensor_1"))
                .keyBy(r -> r.getId())
                .process(new KeyedProcessFunction<String, SensorReading, String>() {

                    private ListState<SensorReading> listState;
                    private ValueState<Long> tsTimer;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        listState = getRuntimeContext().getListState(
                                new ListStateDescriptor<SensorReading>("list-reading", SensorReading.class)
                        );
                        tsTimer = getRuntimeContext().getState(
                                new ValueStateDescriptor<Long>("ts", Types.LONG)
                        );
                    }

                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
                        listState.add(value);
                        if (tsTimer.value() == null) {
                            tsTimer.update(1L);
                            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect("sum:" + listState.get().spliterator().getExactSizeIfKnown());
                        tsTimer.clear();
                    }
                })
                .print();

        env.execute();
    }
}
