package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ListShardResult extends BaseResult {
    @JsonProperty("Shards")
    List<ShardEntry> shards;

    public List<ShardEntry> getShards() {
        return shards;
    }

    public void setShards(List<ShardEntry> shards) {
        this.shards = shards;
    }
}
