package com.atguigu.flink.day4

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object KeyedProcessExampleETime {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        env.socketTextStream("hadoop102", 9999, '\n')
                .map(line => {
                    val data: Array[String] = line.split(" ")
                    (data(0), data(1).toLong * 1000)
                })
                .assignAscendingTimestamps(_._2)
                .keyBy(_._1)
                .process(new MyKeyedProcess)
                .print()

        env.execute()

    }

    class MyKeyedProcess extends KeyedProcessFunction[String, (String, Long), String] {
        override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {
            ctx.timerService().registerEventTimeTimer(value._2 + 10 * 1000L)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
            out.collect("定时器触发  time=" + timestamp)
        }
    }

}
