package com.atguigu.flink.day2

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object MultiStreamTransformations{
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val tampStream: KeyedStream[SensorReading, String] = env.addSource(new SensorSource).keyBy(_.id)
        val smokeStream: DataStream[String] = env.fromElements("LOW", "HIGH")

        tampStream.connect(smokeStream.broadcast)
                        .flatMap(new RaiseAlertFlatMap)
                        .print()

        env.execute()
    }

    case class RaiseAlertFlatMap() extends CoFlatMapFunction[SensorReading,String,Alert]{

        private var smokeLevel = "LOW"

        override def flatMap1(value: SensorReading, out: Collector[Alert]): Unit = {
            if(smokeLevel=="HIGH"&&value.temperature>0.0){
                out.collect(new Alert(value.toString,value.timestamp))
            }
        }

        override def flatMap2(value: String, out: Collector[Alert]): Unit = {
            smokeLevel = value
        }
    }
}
