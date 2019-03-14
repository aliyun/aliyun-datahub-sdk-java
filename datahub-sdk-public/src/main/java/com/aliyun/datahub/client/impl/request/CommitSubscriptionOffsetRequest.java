package com.aliyun.datahub.client.impl.request;

import com.aliyun.datahub.client.model.SubscriptionOffset;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class CommitSubscriptionOffsetRequest extends BaseRequest {
    @JsonProperty("Offsets")
    private Map<String, SubscriptionOffset> offsets;

    public CommitSubscriptionOffsetRequest() {
        setAction("commit");
    }

    public Map<String, SubscriptionOffset> getOffsets() {
        return offsets;
    }

    public CommitSubscriptionOffsetRequest setOffsets(Map<String, SubscriptionOffset> offsets) {
        this.offsets = offsets;
        return this;
    }
}
