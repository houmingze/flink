package com.atguigu.flink.day2;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/11 10:03
 */
public class SensorReading {

    private String id;
    private Long timestamp;
    private Double tamperature;

    public SensorReading(){

    }

    public SensorReading(String id, Long timestamp, Double tamperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.tamperature = tamperature;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTamperature() {
        return tamperature;
    }

    public void setTamperature(Double tamperature) {
        this.tamperature = tamperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", tamperature=" + tamperature +
                '}';
    }
}
