package com.atguigu.flink.day5

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object RedirectLateEvent {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val stream: DataStream[String] = env.socketTextStream("hadoop102", 9999, '\n')
                .map(line => {
                    val data: Array[String] = line.split(" ")
                    (data(0), data(1).toLong * 1000L)
                })
                .assignAscendingTimestamps(_._2)
                .keyBy(_._1)
                .timeWindow(Time.seconds(5))
                .sideOutputLateData(new OutputTag[(String, Long)]("late-reading"))
                .process(new ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
                    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
                        out.collect("一共有" + elements.size)
                    }
                })
        stream.print()
        stream.getSideOutput(new OutputTag[(String,Long)]("late-reading")).print()

        env.execute()

    }

}
