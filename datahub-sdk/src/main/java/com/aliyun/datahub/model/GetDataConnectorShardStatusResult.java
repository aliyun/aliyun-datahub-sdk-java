package com.aliyun.datahub.model;

public class GetDataConnectorShardStatusResult {
    private long startSequence;
    private long endSequence;
    private long curSequence;
    private long updateTime;
    private ConnectorShardStatus state;
    private String lastErrorMessage;
    private long discardCount;

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

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public ConnectorShardStatus getState() {
        return state;
    }

    public void setState(String state) {
        this.state = ConnectorShardStatus.valueOf(state);
    }

    public void setState(ConnectorShardStatus state) {
        this.state = state;
    }

    public String getLastErrorMessage() {
        return lastErrorMessage;
    }

    public void setLastErrorMessage(String lastErrorMessage) {
        this.lastErrorMessage = lastErrorMessage;
    }

    public long getDiscardCount() {
        return discardCount;
    }

    public void setDiscardCount(long discardCount) {
        this.discardCount = discardCount;
    }
}
