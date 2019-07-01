package com.aliyun.datahub.model;

import com.aliyun.datahub.client.model.ConnectorShardStatusEntry;
import com.aliyun.datahub.exception.ResourceNotFoundException;

import java.util.HashMap;
import java.util.Map;

public class GetDataConnectorShardStatusResult extends Result {
    private String shardId;
    private com.aliyun.datahub.client.model.GetConnectorShardStatusResult proxyResult;

    public GetDataConnectorShardStatusResult(String shardId, com.aliyun.datahub.client.model.GetConnectorShardStatusResult proxyResult) {
        this.shardId = shardId;
        this.proxyResult = proxyResult;
        setRequestId(proxyResult.getRequestId());

        if (!proxyResult.getStatusEntryMap().containsKey(shardId)) {
            throw new ResourceNotFoundException("ShardId not found");
        }
    }

    public GetDataConnectorShardStatusResult() {
        this.shardId = "0";
        this.proxyResult = new com.aliyun.datahub.client.model.GetConnectorShardStatusResult();
        Map<String, ConnectorShardStatusEntry> entryMap = new HashMap<>();
        entryMap.put(this.shardId, new ConnectorShardStatusEntry());
    }

    public long getStartSequence() {
        return proxyResult.getStatusEntryMap().get(shardId).getStartSequence();
    }

    public void setStartSequence(long startSequence) {
        proxyResult.getStatusEntryMap().get(shardId).setStartSequence(startSequence);
    }

    public long getEndSequence() {
        return proxyResult.getStatusEntryMap().get(shardId).getEndSequence();
    }

    public void setEndSequence(long endSequence) {
        proxyResult.getStatusEntryMap().get(shardId).setEndSequence(endSequence);
    }

    public long getCurSequence() {
        return proxyResult.getStatusEntryMap().get(shardId).getCurrSequence();
    }

    public void setCurSequence(long curSequence) {
        proxyResult.getStatusEntryMap().get(shardId).setCurrSequence(curSequence);
    }

    public long getUpdateTime() {
        return proxyResult.getStatusEntryMap().get(shardId).getUpdateTime();
    }

    public void setUpdateTime(long updateTime) {
        proxyResult.getStatusEntryMap().get(shardId).setUpdateTime(updateTime);
    }

    public ConnectorShardStatus getState() {
        if (proxyResult.getStatusEntryMap().get(shardId).getState() == null) {
            return null;
        }
        return ConnectorShardStatus.valueOf(proxyResult.getStatusEntryMap().get(shardId).getState().getValue().toUpperCase());
    }

    public void setState(String state) {
        proxyResult.getStatusEntryMap().get(shardId).setState(com.aliyun.datahub.client.model.ConnectorShardState.fromString(state.toUpperCase()));
    }

    public void setState(ConnectorShardStatus state) {
        proxyResult.getStatusEntryMap().get(shardId).setState(com.aliyun.datahub.client.model.ConnectorShardState.fromString(state.name().toUpperCase()));
    }

    public String getLastErrorMessage() {
        return proxyResult.getStatusEntryMap().get(shardId).getLastErrorMessage();
    }

    public void setLastErrorMessage(String lastErrorMessage) {
        proxyResult.getStatusEntryMap().get(shardId).setLastErrorMessage(lastErrorMessage);
    }

    public long getDiscardCount() {
        return proxyResult.getStatusEntryMap().get(shardId).getDiscardCount();
    }

    public void setDiscardCount(long discardCount) {
        proxyResult.getStatusEntryMap().get(shardId).setDiscardCount(discardCount);
    }
}
