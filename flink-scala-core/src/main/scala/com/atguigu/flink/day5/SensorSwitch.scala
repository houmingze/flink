package com.atguigu.flink.day5

import com.atguigu.flink.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SensorSwitch {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val stream: KeyedStream[SensorReading, String] = env.addSource(new SensorSource).keyBy(_.id)
        val switchs: KeyedStream[(String, Long), String] = env.fromElements(("Sensor_2", 10 * 1000L)).keyBy(_._1)

        stream.connect(switchs)
                        .process(new MySwitch)
                        .print()

        env.execute()
    }


    class MySwitch extends CoProcessFunction[SensorReading,(String,Long),SensorReading]{

         lazy val switch: ValueState[Boolean] = getRuntimeContext.getState(
                new ValueStateDescriptor[Boolean]("switch",Types.of[Boolean])
            )

        override def processElement1(value: SensorReading, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, out: Collector[SensorReading]): Unit = {
            if(switch.value()){
                out.collect(value)
            }
        }

        override def processElement2(value: (String, Long), ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, out: Collector[SensorReading]): Unit = {
            switch.update(true)
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+value._2)
        }

        override def onTimer(timestamp: Long, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext, out: Collector[SensorReading]): Unit = {
            super.onTimer(timestamp, ctx, out)
            switch.clear()
        }

    }

}
