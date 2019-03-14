package com.aliyun.datahub.client.model;

import com.aliyun.datahub.client.impl.serializer.ConnectorShardStateDeserializer;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

public class ConnectorShardStatusEntry {
    @JsonProperty("StartSequence")
    private long startSequence;

    @JsonProperty("EndSequence")
    private long endSequence;

    @JsonProperty("CurrentSequence")
    private long currSequence;

    @JsonProperty("CurrentTimestamp")
    private long currTimestamp;

    @JsonProperty("UpdateTime")
    private long updateTime;

    @JsonProperty("State")
    @JsonDeserialize(using = ConnectorShardStateDeserializer.class)
    private ConnectorShardState state;

    @JsonProperty("LastErrorMessage")
    private String lastErrorMessage;

    @JsonProperty("DiscardCount")
    private long discardCount;

    @Deprecated
    public long getStartSequence() {
        return startSequence;
    }

    @Deprecated
    public void setStartSequence(long startSequence) {
        this.startSequence = startSequence;
    }

    @Deprecated
    public long getEndSequence() {
        return endSequence;
    }

    @Deprecated
    public void setEndSequence(long endSequence) {
        this.endSequence = endSequence;
    }

    public long getCurrSequence() {
        return currSequence;
    }

    public void setCurrSequence(long currSequence) {
        this.currSequence = currSequence;
    }

    public long getCurrTimestamp() {
        return currTimestamp;
    }

    public void setCurrTimestamp(long currTimestamp) {
        this.currTimestamp = currTimestamp;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public ConnectorShardState getState() {
        return state;
    }

    public void setState(ConnectorShardState state) {
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
