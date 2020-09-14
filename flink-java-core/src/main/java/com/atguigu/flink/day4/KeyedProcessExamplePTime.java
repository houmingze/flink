package com.atguigu.flink.day4;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/14 16:18
 */
public class KeyedProcessExamplePTime {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String value) throws Exception {
                        String[] data = value.split(" ");
                        return Tuple2.of(data[0], data[1]);
                    }
                })
                .keyBy(r -> r.f0)
                .process(new MyKeyedProcess())
                .print();

        env.execute();
    }

    public static class MyKeyedProcess extends KeyedProcessFunction<String, Tuple2<String, String>, String> {

        @Override
        public void processElement(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("定时器触发 time=" + timestamp);
        }
    }

}
