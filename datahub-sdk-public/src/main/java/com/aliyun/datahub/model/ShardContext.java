package com.aliyun.datahub.model;

public class ShardContext {
    private String shardId;
    private long startSequence;
    private long endSequence;
    private long curSequence;

    public ShardContext() {
        this.curSequence = -1;
        this.startSequence = -1;
        this.endSequence = -1;
    }

    public long getEndSequence() {
        return endSequence;
    }

    public void setEndSequence(long endSequence) {
        this.endSequence = endSequence;
    }

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

    public long getCurSequence() {
        return curSequence;
    }

    public void setCurSequence(long curSequence) {
        this.curSequence = curSequence;
    }
}
