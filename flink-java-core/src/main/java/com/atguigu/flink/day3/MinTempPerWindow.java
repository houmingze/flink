package com.atguigu.flink.day3;

import com.atguigu.flink.day2.SensorReading;
import com.atguigu.flink.day2.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/12 11:52
 */
public class MinTempPerWindow {
    
    public static void main(String[] args) throws  Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> stream = env.addSource(new SensorSource());

        stream.keyBy(r->r.getId()).timeWindow(Time.seconds(5)).min("tamperature").print();

        env.execute();
    }

}
