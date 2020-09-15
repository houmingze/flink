package com.atguigu.flink.day5

import com.atguigu.flink.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutPutExample {

    val output: OutputTag[String] = new OutputTag[String]("side-output")

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val steam: DataStream[SensorReading] = env.addSource(new SensorSource)
                .process(new MyProcess)

        steam.print()
        steam.getSideOutput(output).print()

        env.execute();
    }

    class MyProcess extends ProcessFunction[SensorReading, SensorReading] {
        override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
            if(value.temperature < 32.0){
                ctx.output(output,"传感器id:"+value.id+"的温度小于32度")
            }
            out.collect(value)
        }
    }


}
