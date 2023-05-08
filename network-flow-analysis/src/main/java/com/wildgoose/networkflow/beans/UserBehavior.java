package com.wildgoose.networkflow.beans;

/**
 * @Classname : UserBehavior
 * @Description : 用户行为类
 * @Created by : JustxzzZ
 * @Date : 2023.04.25 11:05
 */
public class UserBehavior {

    private Long userId;        // 用户ID
    private Long itemId;        // 商品ID
    private Integer categoryId; // 商品所属类型ID
    private String behavior;    // 用户行为类型，包含pv、buy、cart、fav
    private Long timestamp;     // 行为发生时间戳，单位秒

    public UserBehavior() {
    }

    public UserBehavior(Long userId, Long itemId, Integer categoryId, String behavior, Long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Integer getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Integer categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "UserId=" + userId +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
