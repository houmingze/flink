package com.atguigu.flink.day6

import com.atguigu.flink.day2.SensorSource
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala._

object StateBackendExcample {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStateBackend(new FsStateBackend("file:///D:\\mySpace\\flink\\checkpoints"))
        env.enableCheckpointing(5000)
        env.addSource(new SensorSource)
                        .print();

        env.execute()
    }

}
