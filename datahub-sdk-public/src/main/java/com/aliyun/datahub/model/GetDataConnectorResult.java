package com.aliyun.datahub.model;

import com.aliyun.datahub.utils.ModelConvertToNew;
import com.aliyun.datahub.utils.ModelConvertToOld;

import java.util.ArrayList;
import java.util.List;

public class GetDataConnectorResult extends Result {
    private com.aliyun.datahub.client.model.GetConnectorResult proxyResult;

    public GetDataConnectorResult(com.aliyun.datahub.client.model.GetConnectorResult proxyResult) {
        this.proxyResult = proxyResult;
        setRequestId(proxyResult.getRequestId());
    }

    public GetDataConnectorResult(){
        proxyResult = new com.aliyun.datahub.client.model.GetConnectorResult();
    }

    public String getState() {
        return proxyResult.getState().name();
    }

    public void setState(String state) {
        proxyResult.setState(com.aliyun.datahub.client.model.ConnectorState.valueOf(state.toUpperCase()));
    }

    public String getCreator() {
        return null;
    }

    public void setCreator(String creator) {
        // nothing
    }

    public String getOwner() {
        return null;
    }

    public void setOwner(String owner) {
        // nothing
    }

    public List<ShardContext> getShardContexts() {
        List<ShardContext> shardContexts = new ArrayList<>();
        if (proxyResult.getShardContexts() != null) {
            for (com.aliyun.datahub.client.model.ShardContext entry : proxyResult.getShardContexts()) {
                ShardContext context = new ShardContext();
                context.setShardId(entry.getShardId());
                context.setCurSequence(entry.getCurSequence());
                context.setStartSequence(entry.getStartSequence());
                context.setEndSequence(entry.getEndSequence());
                shardContexts.add(context);
            }
        }

        return shardContexts;
    }

    public void setShardContext(List<ShardContext> shardContexts) {
        List<com.aliyun.datahub.client.model.ShardContext> newShardContextList = new ArrayList<>();
        for (ShardContext context : shardContexts) {
            com.aliyun.datahub.client.model.ShardContext newContext = new com.aliyun.datahub.client.model.ShardContext();
            newContext.setShardId(context.getShardId());
            newContext.setStartSequence(context.getStartSequence());
            newContext.setCurSequence(context.getCurSequence());
            newContext.setEndSequence(context.getEndSequence());
            newShardContextList.add(newContext);
        }
        proxyResult.setShardContexts(newShardContextList);
    }

    public List<String> getColumnFields() {
        return proxyResult.getColumnFields();
    }

    public void setColumnFields(List<String> columnFields) {
        proxyResult.setColumnFields(columnFields);
    }

    public ConnectorConfig getConnectorConfig() {
        return ModelConvertToOld.convertConnectorConfig(proxyResult.getConfig());
    }

    public void setConnectorConfig(ConnectorConfig config) {
        proxyResult.setConfig(ModelConvertToNew.convertConnectorConfig(config));
    }

    public ConnectorType getType() {
        if (proxyResult.getType() != null) {
            return ConnectorType.valueOf(proxyResult.getType().name().toUpperCase());
        }
        return null;
    }

    public void setType(String type) {
        proxyResult.setType(com.aliyun.datahub.client.model.ConnectorType.valueOf(type.toUpperCase()));
    }
}
