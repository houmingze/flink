package com.atguigu.flink.day4

import com.atguigu.flink.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TempIncreaseAlter {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        env.addSource(new SensorSource)
                .keyBy(_.id)
                .process(new TempAlter)
                .print()

        env.execute()
    }

    class TempAlter extends KeyedProcessFunction[String, SensorReading, String] {
        lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(
            new ValueStateDescriptor[Double]("last-temp", Types.of[Double])
        )

        lazy val timer: ValueState[Long] = getRuntimeContext.getState(
            new ValueStateDescriptor[Long]("timer", Types.of[Long])
        )

        override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
            val preTemp = lastTemp.value()
            val ts = timer.value()
            val temperature: Double = value.temperature
            lastTemp.update(temperature)

            if (preTemp == 0.0 || temperature < preTemp) {
                ctx.timerService().deleteProcessingTimeTimer(ts)
                timer.clear()
            } else if (temperature >= preTemp && ts == 0) {
                val oneSecondLater: Long = ctx.timerService().currentProcessingTime() + 1000L
                timer.update(oneSecondLater)
                ctx.timerService().registerProcessingTimeTimer(oneSecondLater)
            }
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
            super.onTimer(timestamp, ctx, out)
            out.collect("key:" + ctx.getCurrentKey + "，报警了 time=" + timestamp)
        }
    }

}
