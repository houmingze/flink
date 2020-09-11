package com.atguigu.flink.day2

import org.apache.flink.streaming.api.scala._


object SensorStream {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val dStream: DataStream[SensorReading] = env.addSource(new SensorSource)
        dStream.print()

        env.execute()

    }

}
