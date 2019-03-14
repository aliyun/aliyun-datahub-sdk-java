package com.aliyun.datahub.client.impl.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SplitShardRequest extends BaseRequest {
    @JsonProperty("ShardId")
    private String shardId;

    @JsonProperty("SplitKey")
    private String splitKey;

    public SplitShardRequest() {
        setAction("split");
    }

    public String getShardId() {
        return shardId;
    }

    public SplitShardRequest setShardId(String shardId) {
        this.shardId = shardId;
        return this;
    }

    public String getSplitKey() {
        return splitKey;
    }

    public SplitShardRequest setSplitKey(String splitKey) {
        this.splitKey = splitKey;
        return this;
    }
}
