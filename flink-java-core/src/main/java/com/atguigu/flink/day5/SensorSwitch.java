package com.atguigu.flink.day5;

import com.atguigu.flink.day2.SensorReading;
import com.atguigu.flink.day2.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.BoundType;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/15 10:17
 */
public class SensorSwitch {
    
    public static void main(String[] args)throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<SensorReading, String> stream = env.addSource(new SensorSource()).keyBy(r -> r.getId());
        KeyedStream<Tuple2<String, Long>, String> switchs = env.fromElements(Tuple2.of("Sensor_2", 10 * 1000L)).keyBy(r -> r.f0);

        stream.connect(switchs).process(new MySwitch()).print();

        env.execute();
    }

    public static class MySwitch extends CoProcessFunction<SensorReading,Tuple2<String,Long>,SensorReading>{

        private ValueState<Boolean> sensorSwitch;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            sensorSwitch= getRuntimeContext().getState(
                    new ValueStateDescriptor<Boolean>("sensorSwitch", Types.BOOLEAN)
            );
        }

        @Override
        public void processElement1(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            if(sensorSwitch.value()!=null && sensorSwitch.value()){
                out.collect(value);
            }
        }

        @Override
        public void processElement2(Tuple2<String, Long> value, Context ctx, Collector<SensorReading> out) throws Exception {
            sensorSwitch.update(true);
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+value.f1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorReading> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            sensorSwitch.clear();
        }
    }
    
}
