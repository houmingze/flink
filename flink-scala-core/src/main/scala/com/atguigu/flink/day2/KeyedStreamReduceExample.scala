package com.atguigu.flink.day2

import org.apache.flink.streaming.api.scala._

object KeyedStreamReduceExample {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
        stream.filter(_.id == "Sensor_1")
                        .keyBy(_.id)
                        .reduce((t1,t2)=> new SensorReading(t1.id,t1.timestamp,t1.temperature.max(t2.temperature)))
                        .print()

        env.execute()

    }

}
