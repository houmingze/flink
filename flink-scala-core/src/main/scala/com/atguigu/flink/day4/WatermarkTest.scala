package com.atguigu.flink.day4

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WatermarkTest {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.socketTextStream("hadoop102", 9999, '\n')
                .map(r => {
                    val data: Array[String] = r.split(" ")
                    (data(0), data(1).toLong * 1000)
                })
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy.forBoundedOutOfOrderness[(String, Long)](Duration.ofSeconds(5))
                            .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
                                override def extractTimestamp(element: (String, Long), recordTimestamp: Long) = element._2
                            })
                )
                .keyBy(_._1)
                .timeWindow(Time.seconds(5))
                .process(new ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
                    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
                        out.collect(s"key:${key},start:${context.window.getStart},end:${context.window.getEnd},count:${elements.size}")
                    }
                })
                .print()

        env.execute()
    }

}
