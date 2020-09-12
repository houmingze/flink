package com.atguigu.flink.day3;

import com.atguigu.flink.day2.SensorReading;
import com.atguigu.flink.day2.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/12 9:02
 */
public class MultiStreamTransformations {
    
    public static void main(String[] args)throws  Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> tampReadings = env.addSource(new SensorSource());
        DataStream<SmokeLevel> smokeReadings = env.addSource(new SmokeLevelSource());

        tampReadings.keyBy(r->r.getId()).connect(smokeReadings.broadcast()).flatMap(new RaiseAlertFlatMap()).print();

        env.execute();
    }

    public static class RaiseAlertFlatMap implements CoFlatMapFunction<SensorReading,SmokeLevel,Alert>{

        private SmokeLevel level = SmokeLevel.LOW;

        @Override
        public void flatMap1(SensorReading value, Collector<Alert> out) throws Exception {
            if(level == SmokeLevel.HIGH && value.getTamperature() > -100){
                out.collect(new Alert("报警了"+value.getId(),value.getTimestamp()));
            }
        }

        @Override
        public void flatMap2(SmokeLevel value, Collector<Alert> out) throws Exception {
            level = value;
        }
    }
    
}
