package com.atguigu.flink.day2;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/11 10:09
 */
public class SensorStream {
    
    public static void main(String[] args) throws  Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> dStream = env.addSource(new SensorSource());
        dStream.print();

        env.execute();
    }

}
