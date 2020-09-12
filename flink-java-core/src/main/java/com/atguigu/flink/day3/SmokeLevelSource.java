package com.atguigu.flink.day3;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/12 9:00
 */
public class SmokeLevelSource implements SourceFunction<SmokeLevel> {

    private boolean running = true;

    @Override
    public void run(SourceContext<SmokeLevel> ctx) throws Exception {
        Random random = new Random();
        while (running){
            double v = random.nextGaussian();
            if(v > 0.8){
                ctx.collect(SmokeLevel.HIGH);
            }else{
                ctx.collect(SmokeLevel.LOW);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
