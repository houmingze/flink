package com.atguigu.flink.day2

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

object FilterFunctionExample {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
        stream.filter(_.temperature>0)
        stream.filter(new MyFilterFunction)
        stream.filter(new FilterFunction[SensorReading] {
            override def filter(t: SensorReading) = t.temperature > 0
        }).print()

        env.execute()
    }

    class MyFilterFunction extends FilterFunction[SensorReading]{
        override def filter(t: SensorReading): Boolean = t.temperature > 0
    }

}
