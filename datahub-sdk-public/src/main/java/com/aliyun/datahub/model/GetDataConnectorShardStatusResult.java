package com.aliyun.datahub.model;

public class GetDataConnectorShardStatusResult extends Result {
    private com.aliyun.datahub.client.model.ConnectorShardStatusEntry proxyResult;

    public GetDataConnectorShardStatusResult(com.aliyun.datahub.client.model.ConnectorShardStatusEntry proxyResult) {
        this.proxyResult = proxyResult;
    }

    public GetDataConnectorShardStatusResult() {
        this.proxyResult = new com.aliyun.datahub.client.model.ConnectorShardStatusEntry();
    }

    public long getStartSequence() {
        return proxyResult.getStartSequence();
    }

    public void setStartSequence(long startSequence) {
        proxyResult.setStartSequence(startSequence);
    }

    public long getEndSequence() {
        return proxyResult.getEndSequence();
    }

    public void setEndSequence(long endSequence) {
        proxyResult.setEndSequence(endSequence);
    }

    public long getCurSequence() {
        return proxyResult.getCurrSequence();
    }

    public void setCurSequence(long curSequence) {
        proxyResult.setCurrSequence(curSequence);
    }

    public long getUpdateTime() {
        return proxyResult.getUpdateTime();
    }

    public void setUpdateTime(long updateTime) {
        proxyResult.setUpdateTime(updateTime);
    }

    public ConnectorShardStatus getState() {
        if (proxyResult.getState() == null) {
            return null;
        }
        return ConnectorShardStatus.valueOf(proxyResult.getState().getValue().toUpperCase());
    }

    public void setState(String state) {
        proxyResult.setState(com.aliyun.datahub.client.model.ConnectorShardState.fromString(state.toUpperCase()));
    }

    public void setState(ConnectorShardStatus state) {
        proxyResult.setState(com.aliyun.datahub.client.model.ConnectorShardState.fromString(state.name().toUpperCase()));
    }

    public String getLastErrorMessage() {
        return proxyResult.getLastErrorMessage();
    }

    public void setLastErrorMessage(String lastErrorMessage) {
        proxyResult.setLastErrorMessage(lastErrorMessage);
    }

    public long getDiscardCount() {
        return proxyResult.getDiscardCount();
    }

    public void setDiscardCount(long discardCount) {
        proxyResult.setDiscardCount(discardCount);
    }
}
