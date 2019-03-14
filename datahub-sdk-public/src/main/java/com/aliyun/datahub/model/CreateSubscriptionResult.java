package com.aliyun.datahub.model;

public class CreateSubscriptionResult extends Result {
    private String subId;

    public void setSubId(String subId) {
        this.subId = subId;
    }

    public String getSubId() {
        return subId;
    }
}
