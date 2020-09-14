package com.atguigu.flink.day2

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction

import scala.util.Random

class SensorSource extends RichParallelSourceFunction[SensorReading]{

    var running = true

    override def run(ctx: SourceContext[SensorReading]): Unit = {
        val rand = new Random
        val curFTemp = (1 to 10).map(i=>("Sensor_"+i,rand.nextGaussian()*20))
        while(true){
            curFTemp.foreach{
                r =>{
                    ctx.collect(new SensorReading(r._1,System.currentTimeMillis(),r._2 + rand.nextGaussian() * 5))
                }
            }
            Thread.sleep(300)
        }
    }

    override def cancel(): Unit = {
        running = false
    }
}
