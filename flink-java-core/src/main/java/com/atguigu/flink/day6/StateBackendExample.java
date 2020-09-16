package com.atguigu.flink.day6;

import com.atguigu.flink.day2.SensorSource;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/16 15:45
 */
public class StateBackendExample {
    public static void main(String[] args)throws  Exception{
        StreamExecutionEnvironment env=  StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new FsStateBackend("file:///D:\\mySpace\\flink\\checkpoints"));
        env.enableCheckpointing(3000);

        env.addSource(new SensorSource())
                .print();

        env.execute();
    }
}
