package com.wildgoose.networkflow.beans;

/**
 * @Description : url 访问量展示类，展示每个窗口中每个 url 的访问量
 * @Author : JustxzzZ
 * @Date : 2023.04.26 9:31
 */
public class PageViewCount {

    private String url;
    private Long windowEnd;
    private Long count;

    public PageViewCount() {
    }

    public PageViewCount(String url, Long windowEnd, Long count) {
        this.url = url;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
