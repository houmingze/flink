package com.atguigu.flink.day7


import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object OrderTimeOutExample {

    case class OrderEvent(orderId: String, eventType: String, eventTime: Long)

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val stream: KeyedStream[OrderEvent, String] = env.fromElements(
            OrderEvent("1", "create", 2000L),
            OrderEvent("2", "create", 3000L),
            OrderEvent("1", "pay", 4000L)
        ).assignAscendingTimestamps(_.eventTime).keyBy(_.orderId)

        val pattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("create")
                .where(_.eventType == "create")
                .next("pay")
                .where(_.eventType == "pay")
                .within(Time.seconds(5))

        val pStream: PatternStream[OrderEvent] = CEP.pattern(stream, pattern)

        val timeoutTag = new OutputTag[String]("timeout-tag")

        val inFunc = (pattern: scala.collection.Map[String,Iterable[OrderEvent]],out:Collector[String])=>{
            val event: OrderEvent = pattern("create").iterator.next()
            out.collect("orderid :" +event.orderId +"is payed")
        }

        val timeoutFuc = (pattern:scala.collection.Map[String,Iterable[OrderEvent]],ts:Long,out:Collector[String])=>{
            val event: OrderEvent = pattern("create").iterator.next()
            out.collect("orderid:"+event.orderId +"is not payed ts:"+ts)
        }

        val s: DataStream[String] = pStream.flatSelect(timeoutTag)(timeoutFuc)(inFunc)

        s.print();
        s.getSideOutput(timeoutTag).print()

        env.execute();
    }

}
