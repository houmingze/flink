package com.atguigu.flink.day2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/11 11:37
 */
public class FlatMapFunctionExample {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> stream = env.fromElements("white", "black", "gray");

        stream.flatMap(new MyFlatMapFunction());

        stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                if(s.equals("white")) collector.collect(s);
                if(s.equals("black")) collector.collect(s);
            }
        }).print();

        env.execute();
    }

    public static class MyFlatMapFunction implements FlatMapFunction<String,String>{

        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            if(s.equals("white")) collector.collect(s);
            if(s.equals("black")) collector.collect(s);

        }
    }
}
