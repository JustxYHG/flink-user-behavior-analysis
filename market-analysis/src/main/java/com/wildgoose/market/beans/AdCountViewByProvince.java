package com.wildgoose.market.beans;

/**
 * @Description : 展示类-各省份广告推广量
 * @Author : JustxzzZ
 * @Date : 2023.05.04 9:33
 */
public class AdCountViewByProvince {

    private String province;
    private String windowEnd;
    private Long count;

    public AdCountViewByProvince() {
    }

    public AdCountViewByProvince(String province, String windowEnd, Long count) {
        this.province = province;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
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
        return "AdCountViewByProvince{" +
                "province='" + province + '\'' +
                ", windowEnd='" + windowEnd + '\'' +
                ", count=" + count +
                '}';
    }
}
