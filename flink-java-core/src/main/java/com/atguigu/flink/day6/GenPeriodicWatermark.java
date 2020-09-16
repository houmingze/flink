package com.atguigu.flink.day6;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/16 10:09
 */
public class GenPeriodicWatermark {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(5 * 1000L);

        env.socketTextStream("hadoop102",9999)
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] s = value.split(" ");
                        return Tuple2.of(s[0],Long.parseLong(s[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
                            private Long bound  = 10 * 1000L;
                            private Long tsMax = Long.MIN_VALUE + bound + 1;

                            @Override
                            public Watermark getCurrentWatermark() {
                                long mark = tsMax - bound - 1;
                                System.out.println("current auto:" + mark);
                                return new Watermark(mark);
                            }

                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                System.out.println("extract timestamp!");
                                tsMax = Math.max(tsMax,element.f1);
                                return element.f1;
                            }
                        }
                )
                .keyBy(r->r.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("current " + ctx.timerService().currentWatermark());
                    }
                }).print();

        env.execute();
    }
}
