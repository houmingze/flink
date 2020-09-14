package com.atguigu.flink.day3;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/14 8:28
 */
public class MinMax {

    public String key;
    public Double min;
    public Double max;
    public String des;

    public MinMax(){}

    public MinMax(String key, Double min, Double max, String des) {
        this.key = key;
        this.min = min;
        this.max = max;
        this.des = des;
    }

    @Override
    public String toString() {
        return "MinMax{" +
                "key='" + key + '\'' +
                ", min=" + min +
                ", max=" + max +
                ", des='" + des + '\'' +
                '}';
    }
}
