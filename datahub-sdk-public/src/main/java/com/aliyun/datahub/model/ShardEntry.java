package com.aliyun.datahub.model;

import java.util.ArrayList;
import java.util.List;

public class ShardEntry {
    private String shardId;
    private ShardState state;
    private String beginHashKey;
    private String endHashKey;
    private String rightShardId;
    private String leftShardId;
    private List<String> parentShardIds;
    private long closedTime;

    public ShardEntry() {
        parentShardIds = new ArrayList<String>();
        closedTime = 0;
    }

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

    public String getRightShardId() {
        return rightShardId;
    }

    public void setRightShardId(String rightShardId) {
        this.rightShardId = rightShardId;
    }

    public String getLeftShardId() {
        return leftShardId;
    }

    public void setLeftShardId(String leftShardId) {
        this.leftShardId = leftShardId;
    }

    public List<String> getParentShardIds() {
        return parentShardIds;
    }

    public void setParentShardIds(List<String> parentShardIds) {
        this.parentShardIds = parentShardIds;
    }

    public void addParentShardIds(String parentShardId) {
        if (this.parentShardIds == null) {
            this.parentShardIds = new ArrayList<String>();
        }
        this.parentShardIds.add(parentShardId);
    }

    public long getClosedTime() {
        return closedTime;
    }

    public void setClosedTime(long closedTime) {
        this.closedTime = closedTime;
    }
}
