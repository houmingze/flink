package com.atguigu.flink.day1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/9 11:56
 */
public class WordCountFromSocket {

    public static void main(String[] args) throws  Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dStream = env.socketTextStream("hadoop102", 9999);

        KeyedStream<Tuple2<String, Integer>, String> flatDStream = dStream.flatMap(new Tokenizer()).keyBy(r -> r.f0);
        flatDStream.sum(1).print();

        env.execute("");
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] dataStrArr = s.split(" ");
            for (String data : dataStrArr) {
                collector.collect(new Tuple2<>(data,1));
            }
        }
    }


}
