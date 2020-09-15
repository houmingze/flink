package com.atguigu.flink.day5;

import com.atguigu.flink.day2.SensorReading;
import com.atguigu.flink.day2.SensorSource;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/15 9:09
 */
public class SideOutPutExample {

    static OutputTag<String> output = new OutputTag<String>("side-output"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<SensorReading> stream = env.addSource(new SensorSource())
                .process(new MyProcess());
        stream.print();
        stream.getSideOutput(output).print();

        env.execute();
    }

    public static class MyProcess extends ProcessFunction<SensorReading, SensorReading> {

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            if (value.getTamperature() < 32) {
                ctx.output(output, "传感器id:" + value.getId() + "的温度小于32度");
            }
            out.collect(value);
        }
    }
}
