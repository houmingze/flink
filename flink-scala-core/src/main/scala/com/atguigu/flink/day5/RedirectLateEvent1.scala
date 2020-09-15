package com.atguigu.flink.day5

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RedirectLateEvent1 {

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
                .process(new ProcessFunction[(String, Long), String] {
                    val output = new OutputTag[String]("late-reading")
                    override def processElement(value: (String, Long), ctx: ProcessFunction[(String, Long), String]#Context, out: Collector[String]) = {
                        val watermark: Long = ctx.timerService().currentWatermark()
                        if(value._2 < watermark){
                            ctx.output(output,"later data is coming ts=" + value._2)
                        }else{
                            out.collect("no later")
                        }
                    }
                })
        stream.print()
        stream.getSideOutput(new OutputTag[String]("late-reading")).print()

        env.execute()
    }

}
