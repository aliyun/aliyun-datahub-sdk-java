package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class OpenSubscriptionSessionResult extends BaseResult {
    @JsonProperty("Offsets")
    private Map<String, SubscriptionOffset> offsets;

    public Map<String, SubscriptionOffset> getOffsets() {
        return offsets;
    }

    public void setOffsets(Map<String, SubscriptionOffset> offsets) {
        this.offsets = offsets;
    }
}
