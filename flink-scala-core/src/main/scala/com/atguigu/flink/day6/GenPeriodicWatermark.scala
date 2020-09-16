package com.atguigu.flink.day6

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

object GenPeriodicWatermark {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.getConfig.setAutoWatermarkInterval(5 * 1000L)

        env.socketTextStream("hadoop102", 9999, '\n')
                .map(line => {
                    val data: Array[String] = line.split(" ")
                    (data(0), data(1).toLong * 1000L)
                }).assignTimestampsAndWatermarks(
            new AssignerWithPeriodicWatermarks[(String, Long)] {

                val bound = 10 * 1000L
                var tsMax = Long.MinValue + bound + 1

                override def getCurrentWatermark: Watermark = {
                    val mark = tsMax - bound - 1
                    println("current :" + mark)
                    new Watermark(mark)
                }

                override def extractTimestamp(element: (String, Long), recordTimestamp: Long) = {
                    println("extract timestamp!")
                    tsMax = tsMax.max(element._2)
                    element._2
                }
            }
        ).keyBy(_._1)
                .process(new KeyedProcessFunction[String, (String, Long), String] {
                    override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]) = {
                        out.collect("current watermark :" + ctx.timerService().currentWatermark())
                    }
                }).print()

        env.execute()
    }

}
