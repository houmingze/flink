package com.atguigu.flink.day2;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/11 11:31
 */
public class FilterFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> stream = env.addSource(new SensorSource());

        stream.filter(i -> i.getTamperature() > 0);
        stream.filter(new MyFilterFunction());
        stream.filter(new FilterFunction<SensorReading>() {
            @Override
            public boolean filter(SensorReading sensorReading) throws Exception {
                return sensorReading.getTamperature() > 0;
            }
        }).print();


        env.execute();
    }

    public static class MyFilterFunction implements FilterFunction<SensorReading> {

        @Override
        public boolean filter(SensorReading value) throws Exception {
            return value.getTamperature() > 0;
        }
    }
}
