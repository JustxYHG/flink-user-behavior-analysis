package com.wildgoose.market.beans;

/**
 * @Description : 黑名单用户警告信息
 * @Author : JustxzzZ
 * @Date : 2023.05.04 11:05
 */
public class BlacklistUserWarning {

    private Long userId;
    private Long adId;
    private String warningMsg;

    public BlacklistUserWarning() {
    }

    public BlacklistUserWarning(Long userId, Long adId, String warningMsg) {
        this.userId = userId;
        this.adId = adId;
        this.warningMsg = warningMsg;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getAdId() {
        return adId;
    }

    public void setAdId(Long adId) {
        this.adId = adId;
    }

    public String getWarningMsg() {
        return warningMsg;
    }

    public void setWarningMsg(String warningMsg) {
        this.warningMsg = warningMsg;
    }

    @Override
    public String toString() {
        return "BlacklistUserWarning{" +
                "userId=" + userId +
                ", adId=" + adId +
                ", warningMsg='" + warningMsg + '\'' +
                '}';
    }
}
