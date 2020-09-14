package com.atguigu.flink.day4

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object KeyedProcessExamplePTime {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        env.socketTextStream("hadoop102", 9999, '\n')
                .map(line => {
                    val data: Array[String] = line.split(" ")
                    (data(0), data(1))
                })
                .keyBy(_._1)
                .process(new KeyedProcessFunction[String, (String, String), String] {
                    override def processElement(value: (String, String), ctx: KeyedProcessFunction[String, (String, String), String]#Context, out: Collector[String]) = {
                        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10 * 1000L)
                        //println(ctx.timestamp() + "  --  " + ctx.timerService().currentProcessingTime())
                    }

                    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, String), String]#OnTimerContext, out: Collector[String]): Unit = {
                        out.collect("触发定时器 time =" + timestamp)
                    }
                })
                .print()
        env.execute()
    }

}
