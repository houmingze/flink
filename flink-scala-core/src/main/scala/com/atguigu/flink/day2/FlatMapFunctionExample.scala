package com.atguigu.flink.day2

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlatMapFunctionExample {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        val stream: DataStream[String] = env.fromElements("white", "black", "aaaa")
        stream.flatMap(new MyFlatMapFunction)
        stream.flatMap(new FlatMapFunction[String,String] {
            override def flatMap(t: String, collector: Collector[String]): Unit = {
                if(t == "white"){
                    collector.collect( "white")
                }
                if(t == "black"){
                    collector.collect( "black")
                }
            }
        }).print()

        env.execute()

    }

    class MyFlatMapFunction extends FlatMapFunction[String,String]{
        override def flatMap(t: String, collector: Collector[String]): Unit = {
            if(t == "white"){
               collector.collect( "white")
            }
            if(t == "black"){
                collector.collect( "black")
            }
        }
    }

}
