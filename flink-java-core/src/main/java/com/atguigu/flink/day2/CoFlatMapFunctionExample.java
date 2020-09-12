package com.atguigu.flink.day2;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/11 15:50
 */
public class CoFlatMapFunctionExample {
    
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<IdNamePOJO, Integer> stream1 = env.fromElements(new IdNamePOJO(1, "aaa"), new IdNamePOJO(2, "bbb")).keyBy(r -> r.getId());
        KeyedStream<IdNamePOJO, Integer> stream2 = env.fromElements(new IdNamePOJO(1, "ccc"), new IdNamePOJO(2, "ddd")).keyBy(r -> r.getId());
        ConnectedStreams<IdNamePOJO, IdNamePOJO> connect = stream1.connect(stream2);
        connect.flatMap(new MyCoFlatMapFunction()).print();

        env.execute();
    }

    public static class MyCoFlatMapFunction implements CoFlatMapFunction<IdNamePOJO,IdNamePOJO,String>{

        @Override
        public void flatMap1(IdNamePOJO value, Collector<String> out) throws Exception {
            out.collect(value.getName());
            out.collect(value.getName());
        }

        @Override
        public void flatMap2(IdNamePOJO value, Collector<String> out) throws Exception {
            out.collect(value.getName());
        }
    }
}
