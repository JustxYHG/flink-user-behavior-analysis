package com.wildgoose.orderpaydetect.beans;

/**
 * @Description : 订单结果
 * @Author : JustxzzZ
 * @Date : 2023.05.05 10:47
 */
public class OrderResult {

    private Long OrderId;
    private String resultState;

    public OrderResult() {
    }

    public OrderResult(Long orderId, String resultState) {
        OrderId = orderId;
        this.resultState = resultState;
    }

    public Long getOrderId() {
        return OrderId;
    }

    public void setOrderId(Long orderId) {
        OrderId = orderId;
    }

    public String getResultState() {
        return resultState;
    }

    public void setResultState(String resultState) {
        this.resultState = resultState;
    }

    @Override
    public String toString() {
        return "OrderResult{" +
                "OrderId=" + OrderId +
                ", resultState='" + resultState + '\'' +
                '}';
    }
}
