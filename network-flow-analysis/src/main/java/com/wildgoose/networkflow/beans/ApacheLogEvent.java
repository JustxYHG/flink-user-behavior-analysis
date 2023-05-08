package com.wildgoose.networkflow.beans;

/**
 * @Description : 事件日志
 * @Author : JustxzzZ
 * @Date : 2023.04.25 17:49
 */
public class ApacheLogEvent {

    private String ip;      // 访问的 IP
    private Long userId;    // 访问的 userId
    private Long eventTime; // 访问时间
    private String method;  // 访问方式，GET/POST/PUT/DELETE
    private String url;     // 访问的 url

    public ApacheLogEvent() {
    }

    public ApacheLogEvent(String ip, Long userId, Long eventTime, String method, String url) {
        this.ip = ip;
        this.userId = userId;
        this.eventTime = eventTime;
        this.method = method;
        this.url = url;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return "ApacheLogEvent{" +
                "ip='" + ip + '\'' +
                ", userId=" + userId +
                ", eventTime=" + eventTime +
                ", method='" + method + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}
