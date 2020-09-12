package com.atguigu.flink.day3;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/12 11:45
 */
public class RichFunctionExample {

    public static void main(String[] args) throws  Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.fromElements(1,2,3,4)
                .map(new RichMapFunction<Integer, Integer>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("open");
                    }

                    @Override
                    public Integer map(Integer value) throws Exception {
                        return value+1;
                    }

                    @Override
                    public void close() throws Exception {
                        System.out.println("close");
                    }
                })
                .print();


        env.execute();
    }
}
