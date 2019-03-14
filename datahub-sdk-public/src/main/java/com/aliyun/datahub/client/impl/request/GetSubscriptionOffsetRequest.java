package com.aliyun.datahub.client.impl.request;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class GetSubscriptionOffsetRequest extends BaseRequest {
    @JsonProperty("ShardIds")
    private List<String> shardIds;

    public GetSubscriptionOffsetRequest() {
        setAction("get");
    }

    public List<String> getShardIds() {
        return shardIds;
    }

    public GetSubscriptionOffsetRequest setShardIds(List<String> shardIds) {
        this.shardIds = shardIds;
        return this;
    }
}
