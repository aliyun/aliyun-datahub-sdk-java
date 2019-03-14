package com.aliyun.datahub.client.impl.request;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class OpenSubscriptionSessionRequest extends BaseRequest {
    @JsonProperty("ShardIds")
    private List<String> shardIds;

    public OpenSubscriptionSessionRequest() {
        setAction("open");
    }

    public List<String> getShardIds() {
        return shardIds;
    }

    public OpenSubscriptionSessionRequest setShardIds(List<String> shardIds) {
        this.shardIds = shardIds;
        return this;
    }
}
