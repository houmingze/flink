package com.atguigu.flink.day3

import com.atguigu.flink.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object MinMaxTempAggWindow {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        env.addSource(new SensorSource)
                        .keyBy(_.id)
                        .timeWindow(Time.seconds(5))
                        .aggregate(new Agg,new Win)
                        .print()
        env.execute()
    }

    class Agg extends AggregateFunction[SensorReading,(Double,Double),(Double,Double)]{
        override def createAccumulator(): (Double, Double) = (0.0,0.0)

        override def add(value: SensorReading, accumulator: (Double, Double)): (Double, Double) = {
            (accumulator._1.min(value.temperature),accumulator._2.max(value.temperature))
        }

        override def getResult(accumulator: (Double, Double)): (Double, Double) = accumulator

        override def merge(a: (Double, Double), b: (Double, Double)): (Double, Double) = (a._1.min(b._1),a._2.max(b._2))
    }
    class Win extends ProcessWindowFunction[(Double,Double),MinMax,String,TimeWindow]{
        override def process(key: String, context: Context, elements: Iterable[(Double, Double)], out: Collector[MinMax]): Unit = {
            val start: Long = context.window.getStart
            val end: Long = context.window.getEnd
            out.collect(new MinMax(key,elements.head._1,elements.head._2,s"start:${start},end:${end}"))
        }
    }

    case class MinMax(key:String,min:Double,max:Double,out:String)
}
