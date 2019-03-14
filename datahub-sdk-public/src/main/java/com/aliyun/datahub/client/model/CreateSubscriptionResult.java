package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CreateSubscriptionResult extends BaseResult {
    @JsonProperty("SubId")
    private String subId;

    public String getSubId() {
        return subId;
    }

    public void setSubId(String subId) {
        this.subId = subId;
    }
}
