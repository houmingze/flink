package com.atguigu.flink.day5

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object UpdateWindowResultLateEvent {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        env.socketTextStream("hadoop102", 9999, '\n')
                .map(line => {
                    val data: Array[String] = line.split(" ")
                    (data(0), data(1).toLong * 1000L)
                })
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy.forBoundedOutOfOrderness[(String, Long)](Duration.ofSeconds(5))
                            .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
                                override def extractTimestamp(element: (String, Long), recordTimestamp: Long) = {
                                    element._2
                                }
                            })
                )
                .keyBy(_._1)
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(5))
                .process(new ProcessWindowFunction[(String, Long), String, String, TimeWindow] {

                    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
                        val isUpdate: ValueState[Boolean] = context.windowState.getState(
                            new ValueStateDescriptor[Boolean]("is-update", Types.of[Boolean])
                        )
                        if (!isUpdate.value()) {
                            out.collect("first")
                            isUpdate.update(true)
                        } else {
                            out.collect("update result")
                        }
                    }
                })
                .print()


        env.execute()
    }

}
