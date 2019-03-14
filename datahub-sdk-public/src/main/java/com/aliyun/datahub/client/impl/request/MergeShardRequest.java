package com.aliyun.datahub.client.impl.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MergeShardRequest extends BaseRequest {
    @JsonProperty("ShardId")
    private String shardId;

    @JsonProperty("AdjacentShardId")
    private String adjacentShardId;

    public MergeShardRequest() {
        setAction("merge");
    }

    public String getShardId() {
        return shardId;
    }

    public MergeShardRequest setShardId(String shardId) {
        this.shardId = shardId;
        return this;
    }

    public String getAdjacentShardId() {
        return adjacentShardId;
    }

    public MergeShardRequest setAdjacentShardId(String adjacentShardId) {
        this.adjacentShardId = adjacentShardId;
        return this;
    }
}
