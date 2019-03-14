package com.aliyun.datahub.client.impl.request;

import com.aliyun.datahub.client.impl.serializer.ResetSubscriptionOffsetRequestSerializer;
import com.aliyun.datahub.client.model.SubscriptionOffset;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Map;

@JsonSerialize(using = ResetSubscriptionOffsetRequestSerializer.class)
public class ResetSubscriptionOffsetRequest extends BaseRequest {
    private Map<String, SubscriptionOffset> offsets;

    public ResetSubscriptionOffsetRequest() {
        setAction("reset");
    }

    public Map<String, SubscriptionOffset> getOffsets() {
        return offsets;
    }

    public ResetSubscriptionOffsetRequest setOffsets(Map<String, SubscriptionOffset> offsets) {
        this.offsets = offsets;
        return this;
    }
}
