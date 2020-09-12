package com.atguigu.flink.day2

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CoFlatMapFunctionExample {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val stream1: KeyedStream[(Int, String), Int] = env.fromElements((1, "aaa"), (2, "bbb")).keyBy(_._1)
        val stream2: KeyedStream[(Int, String), Int] = env.fromElements((1, "ccc"), (2, "ddd")).keyBy(_._1)

        val connectedStream: ConnectedStreams[(Int, String), (Int, String)] = stream1.connect(stream2)

        connectedStream.flatMap(new MyCoFlatMapFunction)
                        .print()

        env.execute()
    }

    class MyCoFlatMapFunction extends  CoFlatMapFunction[(Int,String),(Int,String),String]{

        override def flatMap1(value: (Int, String), out: Collector[String]): Unit = {
            out.collect(value._2)
            out.collect(value._2)
        }

        override def flatMap2(value: (Int, String), out: Collector[String]): Unit = {
            out.collect(value._2)
        }
    }
}
