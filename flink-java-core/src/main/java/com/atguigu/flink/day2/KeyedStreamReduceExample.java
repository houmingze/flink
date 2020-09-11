package com.atguigu.flink.day2;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/11 14:36
 */
public class KeyedStreamReduceExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> stream = env.addSource(new SensorSource());
        stream.filter(i -> i.getId().equals("Sensor_1"))
                .keyBy(i -> i.getId())
                .reduce((r1, r2) -> {
                    if (r1.getTamperature() >= r2.getTamperature()) {
                        return r1;
                    } else {
                        return r2;
                    }
                });

        stream.filter(i -> i.getId().equals("Sensor_1"))
                .keyBy(i -> i.getId())
                .reduce(new MyReduceFunction())
                .print();

        env.execute();
    }

    public static class MyReduceFunction implements ReduceFunction<SensorReading>{

        @Override
        public SensorReading reduce(SensorReading sensorSource, SensorReading t1) throws Exception {
            if(sensorSource.getTamperature() >= t1.getTamperature()){
                return sensorSource;
            }else{
                return t1;
            }
        }
    }

}
