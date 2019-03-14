package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class SplitShardResult extends BaseResult {
    @JsonProperty("NewShards")
    private List<ShardEntry> newShards;

    public List<ShardEntry> getNewShards() {
        return newShards;
    }

    public void setNewShards(List<ShardEntry> newShards) {
        this.newShards = newShards;
    }
}
