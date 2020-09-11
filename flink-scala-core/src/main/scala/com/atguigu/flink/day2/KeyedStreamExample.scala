package com.atguigu.flink.day2

import org.apache.flink.streaming.api.scala._

object KeyedStreamExample {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
        stream.filter(_.id == "Sensor_1")
                        .keyBy(_.id)
                        .max(2)
                        .print()

        env.execute()

    }

}
