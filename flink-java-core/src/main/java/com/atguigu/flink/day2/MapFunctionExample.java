package com.atguigu.flink.day2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/11 11:26
 */
public class MapFunctionExample {
    
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> stream = env.addSource(new SensorSource());
        stream.map(i->i.getId());
        stream.map(new MyMapFunction());
        stream.map(new MapFunction<SensorReading, String>() {
            @Override
            public String map(SensorReading sensorReading) throws Exception {
                return sensorReading.getId();
            }
        }).print();


        env.execute();
    }

    public static class MyMapFunction implements MapFunction<SensorReading,String>{

        @Override
        public String map(SensorReading sensorReading) throws Exception {
            return sensorReading.getId();
        }
    }
    
}
