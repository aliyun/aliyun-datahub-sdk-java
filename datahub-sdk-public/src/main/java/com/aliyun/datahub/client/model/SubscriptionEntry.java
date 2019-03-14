package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SubscriptionEntry extends BaseResult {
    @JsonProperty("SubId")
    private String subId;
    @JsonProperty("Comment")
    private String comment;
    @JsonProperty("Type")
    private SubscriptionType type;
    @JsonProperty("State")
    private SubscriptionState state;
    @JsonProperty("CreateTime")
    private long createTime;
    @JsonProperty("LastModifyTime")
    private long lastModifyTime;

    public String getSubId() {
        return subId;
    }

    public void setSubId(String subId) {
        this.subId = subId;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public SubscriptionType getType() {
        return type;
    }

    public void setType(SubscriptionType type) {
        this.type = type;
    }

    public SubscriptionState getState() {
        return state;
    }

    public void setState(SubscriptionState state) {
        this.state = state;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getLastModifyTime() {
        return lastModifyTime;
    }

    public void setLastModifyTime(long lastModifyTime) {
        this.lastModifyTime = lastModifyTime;
    }
}
