package com.atguigu.flink.day3;

import com.atguigu.flink.day2.SensorReading;
import com.atguigu.flink.day2.SensorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/14 8:37
 */
public class MinMaxTempAggWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new SensorSource())
                .keyBy(r -> r.getId())
                .timeWindow(Time.seconds(5))
                .aggregate(new Agg(), new Win())
                .print();

        env.execute();
    }

    public static class Agg implements AggregateFunction<SensorReading, Tuple2<Double, Double>, Tuple2<Double, Double>> {

        @Override
        public Tuple2<Double, Double> createAccumulator() {
            return Tuple2.of(Double.MAX_VALUE, Double.MIN_VALUE);
        }

        @Override
        public Tuple2<Double, Double> add(SensorReading value, Tuple2<Double, Double> accumulator) {
            accumulator.f0.min(value.getTamperature(), accumulator.f0);
            accumulator.f1.max(value.getTamperature(), accumulator.f1);
            return accumulator;
        }

        @Override
        public Tuple2<Double, Double> getResult(Tuple2<Double, Double> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<Double, Double> merge(Tuple2<Double, Double> a, Tuple2<Double, Double> b) {
            return null;
        }
    }

    public static class Win extends ProcessWindowFunction<Tuple2<Double, Double>, MinMax, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Tuple2<Double, Double>> elements, Collector<MinMax> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Tuple2<Double, Double> element = null;
            for (Tuple2<Double, Double> e : elements) {
                if (element != null) return;
                element = e;
            }

            out.collect(new MinMax(s, element.f0, element.f1, "start:" + start + ",end:" + end));
        }
    }
}
