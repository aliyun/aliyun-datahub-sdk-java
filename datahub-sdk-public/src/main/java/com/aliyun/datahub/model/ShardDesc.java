package com.aliyun.datahub.model;

/**
 * Created by dongxiao on 16-3-28.
 */
public class ShardDesc {
    private String shardId;
    private String beginHashKey;
    private String endHashKey;

    public ShardDesc() { }

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
