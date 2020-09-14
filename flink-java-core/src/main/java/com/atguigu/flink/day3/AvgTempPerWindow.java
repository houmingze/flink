package com.atguigu.flink.day3;

import com.atguigu.flink.day2.SensorReading;
import com.atguigu.flink.day2.SensorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/12 15:15
 */
public class AvgTempPerWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new SensorSource())
                .keyBy(r -> r.getId())
                .timeWindow(Time.seconds(5))
                .aggregate(new AggregateFunction<SensorReading, Tuple3<String, Double, Long>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple3<String, Double, Long> createAccumulator() {
                        return Tuple3.of("", 0.0, 0L);
                    }

                    @Override
                    public Tuple3<String, Double, Long> add(SensorReading value, Tuple3<String, Double, Long> accumulator) {
                        return Tuple3.of(value.getId(), value.getTamperature() + accumulator.f1, accumulator.f2 + 1);
                    }

                    @Override
                    public Tuple2<String, Double> getResult(Tuple3<String, Double, Long> accumulator) {
                        return Tuple2.of(accumulator.f0, accumulator.f1 / accumulator.f2);
                    }

                    @Override
                    public Tuple3<String, Double, Long> merge(Tuple3<String, Double, Long> a, Tuple3<String, Double, Long> b) {
                        return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2);
                    }
                }).print();

        env.execute();
    }

}
