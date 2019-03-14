package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ShardEntry {
    @JsonProperty("ShardId")
    private String shardId;

    @JsonProperty("State")
    private ShardState state;

    @JsonProperty("ClosedTime")
    private long closedTime;

    @JsonProperty("BeginHashKey")
    private String beginHashKey;

    @JsonProperty("EndHashKey")
    private String endHashKey;

    @JsonProperty("ParentShardIds")
    private List<String> parentShardIds;

    @JsonProperty("LeftShardId")
    private String leftShardId;

    @JsonProperty("RightShardId")
    private String rightShardId;

    @JsonProperty("Address")
    private String address;

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    public ShardState getState() {
        return state;
    }

    public void setState(ShardState state) {
        this.state = state;
    }

    public long getClosedTime() {
        return closedTime;
    }

    public void setClosedTime(long closedTime) {
        this.closedTime = closedTime;
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

    public List<String> getParentShardIds() {
        return parentShardIds;
    }

    public void setParentShardIds(List<String> parentShardIds) {
        this.parentShardIds = parentShardIds;
    }

    public String getLeftShardId() {
        return leftShardId;
    }

    public void setLeftShardId(String leftShardId) {
        this.leftShardId = leftShardId;
    }

    public String getRightShardId() {
        return rightShardId;
    }

    public void setRightShardId(String rightShardId) {
        this.rightShardId = rightShardId;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}

