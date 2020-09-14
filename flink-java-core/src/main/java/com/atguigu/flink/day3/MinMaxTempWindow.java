package com.atguigu.flink.day3;

import com.atguigu.flink.day2.SensorReading;
import com.atguigu.flink.day2.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/14 8:27
 */
public class MinMaxTempWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> stream = env.addSource(new SensorSource());
        stream.keyBy(r -> r.getId()).timeWindow(Time.seconds(5)).process(new ProcessWindowFunction<SensorReading, Object, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<SensorReading> elements, Collector<Object> out) throws Exception {
                long start = context.window().getStart();
                long end = context.window().getEnd();
                double min = Double.MAX_VALUE;
                double max = Double.MIN_VALUE;
                for (SensorReading element : elements) {
                    if (element.getTamperature() < min) {
                        min = element.getTamperature();
                    }
                    if (element.getTamperature() > max) {
                        max = element.getTamperature();
                    }
                }
                out.collect(new MinMax(s, min, max, "start:" + start + ",end:" + end));
            }
        }).print();
        env.execute();
    }
}
