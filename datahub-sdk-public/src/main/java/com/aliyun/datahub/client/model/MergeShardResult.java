package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MergeShardResult extends BaseResult {
    @JsonProperty("ShardId")
    private String shardId;

    @JsonProperty("BeginHashKey")
    private String beginHashKey;

    @JsonProperty("EndHashKey")
    private String endHashKey;

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    public String getBeginHashKey() {
        return beginHashKey;
    }

    public void setBeginHashKey(String beginHashKey) {
        this.beginHashKey = beginHashKey;
    }

    public String getEndHashKey() {
        return endHashKey;
    }

    public void setEndHashKey(String endHashKey) {
        this.endHashKey = endHashKey;
    }
}
