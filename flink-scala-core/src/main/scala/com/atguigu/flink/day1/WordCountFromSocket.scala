package com.atguigu.flink.day1

import com.atguigu.flink.day1.WordCountFromBatch.WordCount
import org.apache.flink.streaming.api.scala._

object WordCountFromSocket {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val stream: DataStream[WordCount] = env.socketTextStream("hadoop102", 9999, '\n')
                .flatMap(_.split(" "))
                .map(WordCount(_, 1))
                .keyBy(_.word)
                .sum("count")

        stream.print()

        env.execute()

    }

}
