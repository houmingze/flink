package com.atguigu.flink.day6

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object TwoWindowJoinExample {

    def main(args: Array[String]): Unit = {
        val env= StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val stream1: DataStream[(String, Long)] = env.fromElements(("a", 1000L), ("a", 2000L), ("b", 1000L), ("b", 2000L))
                .assignAscendingTimestamps(_._2)


        val stream2: DataStream[(String, Long)] = env.fromElements(("a", 3000L), ("a", 4000L), ("b", 3000L), ("b", 4000L))
                .assignAscendingTimestamps(_._2)

        stream1.join(stream2)
                        .where(_._1)
                        .equalTo(_._1)
                        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                        .apply(
                            new JoinFunction[(String,Long),(String,Long),String] {
                                override def join(first: (String, Long), second: (String, Long)) = {
                                    first + "=>" +second
                                }
                            }
                        ).print()

        env.execute()
    }

}
