package com.atguigu.flink.day3

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object RichFunctionExample {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val stream: DataStream[Int] = env.fromElements(1, 2, 3)
        stream.map(new MyRichMapFunctrion).print()

        env.execute();
    }

    class MyRichMapFunctrion extends RichMapFunction[Int,Int]{

        override def open(parameters: Configuration): Unit = {
            println("open")
        }

        override def map(value: Int): Int = {
            value+1
        }

        override def close(): Unit = {
            println("close")
        }
    }

}
