package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ShardContext {
    @JsonProperty("ShardId")
    private String shardId;

    @JsonProperty("StartSequence")
    private long startSequence;

    @JsonProperty("EndSequence")
    private long endSequence;

    @JsonProperty("CurrentSequence")
    private long curSequence;

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    public long getStartSequence() {
        return startSequence;
    }

    public void setStartSequence(long startSequence) {
        this.startSequence = startSequence;
    }

    public long getEndSequence() {
        return endSequence;
    }

    public void setEndSequence(long endSequence) {
        this.endSequence = endSequence;
    }

    public long getCurSequence() {
        return curSequence;
    }

    public void setCurSequence(long curSequence) {
        this.curSequence = curSequence;
    }
}
