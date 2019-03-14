package com.aliyun.datahub.client.impl.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ReloadConnectorRequest extends BaseRequest {
    @JsonProperty("ShardId")
    private String shardId;

    public ReloadConnectorRequest() {
        setAction("Reload");
    }

    public String getShardId() {
        return shardId;
    }

    public ReloadConnectorRequest setShardId(String shardId) {
        this.shardId = shardId;
        return this;
    }
}
