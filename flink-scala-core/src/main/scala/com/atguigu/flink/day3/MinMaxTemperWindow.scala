package com.atguigu.flink.day3

import com.atguigu.flink.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object MinMaxTemperWindow {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
        stream.keyBy(_.id)
                .timeWindow(Time.seconds(5))
                        .process(new ProcessWindowFunction[SensorReading,MinMax,String,TimeWindow] {
                            override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[MinMax]): Unit = {
                                val start: Long = context.window.getStart
                                val end: Long = context.window.getEnd
                                var min = Double.MaxValue
                                var max = Double.MinValue
                                elements.foreach(e=> {
                                    if (e.temperature < min) {
                                        min = e.temperature
                                    }
                                    if (e.temperature > max) {
                                        max = e.temperature
                                    }
                                }
                                )
                                out.collect(new MinMax(key,min,max,s"start:${start},end:${end}"))
                            }
                        }).print()
        env.execute();
    }

    case class MinMax(key:String,min:Double,max:Double,out:String)

}
