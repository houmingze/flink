package com.atguigu.flink.day2;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/11 10:05
 */
public class SensorSource extends RichParallelSourceFunction<SensorReading> {

    private boolean running = true;
    private Random random = new Random();

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        while (true) {
            for (int i = 1; i <= 10; i++) {
                ctx.collect(new SensorReading("Sensor_" + i, System.currentTimeMillis(), getRandomTemperature()));
            }
            Thread.sleep(300);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    private Double getRandomTemperature() {
        return random.nextGaussian() * 20 + random.nextGaussian() * 5;
    }
}
