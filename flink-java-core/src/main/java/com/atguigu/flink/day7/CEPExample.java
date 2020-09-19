package com.atguigu.flink.day7;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;


/**
 * @author ：hmz
 * @date ：Created in 2020/9/18 14:54
 */
public class CEPExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        KeyedStream<LoginEvent, String> stream = env.fromElements(
                new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                new LoginEvent("user_1", "192.168.0.4", "fail", 2000L),
                new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                new LoginEvent("user_1", "192.168.0.3", "fail", 4000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<LoginEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<LoginEvent>) (e, r) -> e.eventTime)
        ).keyBy(r -> r.userId);

        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("first")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                }).next("second")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                }).next("third")
                .where(new SimpleCondition<LoginEvent>() {
                            @Override
                            public boolean filter(LoginEvent value) throws Exception {
                                return value.eventType.equals("fail");
                            }
                        }).within(Time.seconds(5));

        PatternStream<LoginEvent> pStream = CEP.pattern(stream, pattern);
        pStream.select(new PatternSelectFunction<LoginEvent, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> select(Map<String, List<LoginEvent>> map) throws Exception {
                System.out.println(map.toString());
                LoginEvent first = map.get("first").get(0);
                LoginEvent second = map.get("second").get(0);
                LoginEvent third = map.get("third").get(0);
                return Tuple4.of(first.userId, first.ipAddress, second.ipAddress, third.ipAddress);
            }
        }).print();

        env.execute();
    }
}
