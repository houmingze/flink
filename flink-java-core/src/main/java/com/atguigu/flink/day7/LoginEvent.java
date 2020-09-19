package com.atguigu.flink.day7;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/18 15:08
 */
public class LoginEvent {

    public String userId;
    public String ipAddress;
    public String eventType;
    public Long eventTime;

    public LoginEvent() {
    }

    public LoginEvent(String userId, String ipAddress, String eventType, Long eventTime) {
        this.userId = userId;
        this.ipAddress = ipAddress;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}
