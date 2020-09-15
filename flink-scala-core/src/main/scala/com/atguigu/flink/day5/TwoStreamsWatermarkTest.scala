package com.atguigu.flink.day5

import java.time.Duration

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TwoStreamsWatermarkTest {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val stream1: DataStream[(String, Long)] = env.socketTextStream("hadoop102", 9999, '\n')
                .map(line => {
                    val data: Array[String] = line.split(" ")
                    (data(0), data(1).toLong * 1000L)
                })
                .assignAscendingTimestamps(_._2)
        val stream2: DataStream[(String, Long)] = env.socketTextStream("hadoop102", 9998, '\n')
                .map(line => {
                    val data: Array[String] = line.split(" ")
                    (data(0), data(1).toLong * 1000L)
                })
                .assignAscendingTimestamps(_._2)
        stream1.union(stream2)
                .keyBy(_._1)
                        .process(new KeyedProcessFunction[String,(String,Long),String] {
                            override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]) = {
                                out.collect("水位线来了"+ctx.timerService().currentWatermark())
                            }
                        }).print()

        env.execute();
    }

}
