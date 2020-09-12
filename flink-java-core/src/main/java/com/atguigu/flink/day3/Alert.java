package com.atguigu.flink.day3;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/12 8:58
 */
public class Alert {

    private String message;
    private Long timeStamp;

    public Alert(){};

    public Alert(String message, Long timeStamp) {
        this.message = message;
        this.timeStamp = timeStamp;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "Alert{" +
                "message='" + message + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
