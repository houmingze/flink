package com.atguigu.flink.day5;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/15 14:41
 */
public class RedirectLateEvent2 {
    
    public static void main(String[] args) throws Exception{
     StreamExecutionEnvironment env  =   StreamExecutionEnvironment.getExecutionEnvironment();
     env.setParallelism(1);
     env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<String> stream = env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] data = value.split(" ");
                        return Tuple2.of(data[0], Long.parseLong(data[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                                return element.f1;
                                            }
                                        }
                                )
                ).process(new ProcessFunction<Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        long watermark = ctx.timerService().currentWatermark();
                        if (value.f1.longValue()< watermark ) {
                            ctx.output(new OutputTag<String>("late-reading"){}, "late event ts=" + value.f1);
                        } else {
                            out.collect("no later");
                        }
                    }
                });

        stream.print();
        stream.getSideOutput(new OutputTag<String>("late-reading")).print();

        env.execute();
    }
}
