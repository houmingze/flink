package com.atguigu.flink.day7

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object CEPExample {

    case class LoginEvent(userId: String,
                          ip: String,
                          eventType: String,
                          eventTime: Long)

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
        val stream: KeyedStream[LoginEvent, String] = env.fromElements(
            LoginEvent("1", "192.168.0.1", "fail", 2000L),
            LoginEvent("1", "192.168.0.2", "fail", 3000L),
            LoginEvent("1", "192.168.0.3", "fail", 4000L),
            LoginEvent("2", "192.168.10.10", "success", 5000L)
        ).assignAscendingTimestamps(_.eventTime)
                .keyBy(_.userId)

        val pattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("first")
                .where(_.eventType == "fail")
                .next("second")
                .where(_.eventType == "fail")
                .next("third")
                .where(_.eventType == "fail")
                .within(Time.seconds(10))

        val pattern1 = Pattern.begin[LoginEvent]("first").times(3).where(_.eventType=="fail").within(Time.seconds(5))

        val patternStream: PatternStream[LoginEvent] = CEP.pattern(stream, pattern)
        val patternStream1: PatternStream[LoginEvent] = CEP.pattern(stream, pattern1)

        patternStream.select(
            (pattern: scala.collection.Map[String, Iterable[LoginEvent]]) => {
                var first = pattern("first").iterator.next()
                var second = pattern("second").iterator.next()
                var third = pattern("third").iterator.next()

                (first.userId, first.ip, second.ip, third.ip)
            }

        )//.print()
        patternStream1.select(
            (pattern: scala.collection.Map[String, Iterable[LoginEvent]]) => {0
                var first = pattern("first").iterator
                for (elem <- first) {
                    println(elem)
                }
                ("cep","done")
            }
        ).print()

        env.execute()
    }

}
