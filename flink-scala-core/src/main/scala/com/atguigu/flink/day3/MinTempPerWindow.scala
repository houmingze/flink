package com.atguigu.flink.day3

import com.atguigu.flink.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object MinTempPerWindow {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(1)
        val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
        stream.keyBy(_.id).timeWindow(Time.seconds(5)).min(2).print()

        env.execute()
    }

}
