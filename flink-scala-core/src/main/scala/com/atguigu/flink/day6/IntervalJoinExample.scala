package com.atguigu.flink.day6

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object IntervalJoinExample {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val stream1: KeyedStream[(String, Long, String), String] = env.fromElements(
            ("user_1", 10 * 60 * 1000L, "click")
        ).assignAscendingTimestamps(_._2)
                .keyBy(_._1)

        val stream2: KeyedStream[(String, Long, String), String] = env.fromElements(
            ("user_1", 5 * 60 * 1000L, "browse"),
            ("user_1", 6 * 60 * 1000L, "browse")
        ).assignAscendingTimestamps(_._2)
                .keyBy(_._1)

      /*  stream1.intervalJoin(stream2)
                        .between(Time.minutes(-10),Time.minutes(0))
                        .process(new ProcessJoinFunction[(String,Long,String),(String,Long,String),String] {
                            override def processElement(left: (String, Long, String), right: (String, Long, String), ctx: ProcessJoinFunction[(String, Long, String), (String, Long, String), String]#Context, out: Collector[String]) = {
                                out.collect(left + " = > " +right)
                            }
                        }).print()*/
        stream2.intervalJoin(stream1)
                        .between(Time.minutes(0),Time.minutes(10))
                        .process(new ProcessJoinFunction[(String,Long,String),(String,Long,String),String] {
                            override def processElement(left: (String, Long, String), right: (String, Long, String), ctx: ProcessJoinFunction[(String, Long, String), (String, Long, String), String]#Context, out: Collector[String]) = {
                                out.collect(left + " = > " +right)
                            }
                        }).print()
        env.execute()
    }

}
