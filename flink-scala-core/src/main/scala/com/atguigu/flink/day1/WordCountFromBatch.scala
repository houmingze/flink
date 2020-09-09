package com.atguigu.flink.day1

import org.apache.flink.streaming.api.scala._

object WordCountFromBatch {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val stream = env.fromElements("Hello World","Hello World")
                        .flatMap(r=>r.split("\\s"))
                        .map(WordCount(_,1))
                        .keyBy(_.word)
                        .sum(1)

        stream.print()

        env.execute()

    }

    case class WordCount(word:String,count:Int)


}
