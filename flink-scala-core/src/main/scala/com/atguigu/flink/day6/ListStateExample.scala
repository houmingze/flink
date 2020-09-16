package com.atguigu.flink.day6

import java.lang

import com.atguigu.flink.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ListStateExample {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        env.addSource(new SensorSource)
                .filter(_.id=="Sensor_1")
                        .keyBy(_.id)
                        .process(new KeyedProcessFunction[String,SensorReading,String] {

                            lazy val listState :ListState[SensorReading] = getRuntimeContext.getListState(
                                new ListStateDescriptor[SensorReading]("list-reading",classOf[SensorReading])
                            )

                            lazy val tsTimer : ValueState[Long] = getRuntimeContext.getState(
                                new ValueStateDescriptor[Long] ("ts",Types.of[Long])
                            )

                            override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]) = {
                                listState.add(value)
                                if(tsTimer.value() == null){
                                    tsTimer.update(1L)
                                    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L)
                                }
                            }

                            override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
                                super.onTimer(timestamp, ctx, out)
                                val readings: lang.Iterable[SensorReading] = listState.get()
                                out.collect("sum:"+readings.spliterator().getExactSizeIfKnown)
                                tsTimer.clear()
                            }
                        })
                        .print()
        env.execute()
    }

}
