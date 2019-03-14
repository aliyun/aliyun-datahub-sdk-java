package com.aliyun.datahub.client.impl.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GetConnectorShardStatusRequest extends BaseRequest {
    @JsonProperty("ShardId")
    private String shardId;

    public GetConnectorShardStatusRequest() {
        setAction("Status");
    }

    public String getShardId() {
        return shardId;
    }

    public GetConnectorShardStatusRequest setShardId(String shardId) {
        this.shardId = shardId;
        return this;
    }
}
