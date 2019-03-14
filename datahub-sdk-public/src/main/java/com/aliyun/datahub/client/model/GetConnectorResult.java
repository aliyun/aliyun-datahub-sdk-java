package com.aliyun.datahub.client.model;

import com.aliyun.datahub.client.impl.serializer.GetConnectorResultDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.List;

@JsonDeserialize(using = GetConnectorResultDeserializer.class)
public class GetConnectorResult extends BaseResult {
    private ConnectorType type;
    private ConnectorState state;
    private List<String> columnFields;
    private SinkConfig config;

    @Deprecated
    private List<ShardContext> shardContexts;
    private String subId;

    public ConnectorType getType() {
        return type;
    }

    public void setType(ConnectorType type) {
        this.type = type;
    }

    public ConnectorState getState() {
        return state;
    }

    public void setState(ConnectorState state) {
        this.state = state;
    }

    public List<String> getColumnFields() {
        return columnFields;
    }

    public void setColumnFields(List<String> columnFields) {
        this.columnFields = columnFields;
    }

    public SinkConfig getConfig() {
        return config;
    }

    public void setConfig(SinkConfig config) {
        this.config = config;
    }

    @Deprecated
    public List<ShardContext> getShardContexts() {
        return shardContexts;
    }

    @Deprecated
    public void setShardContexts(List<ShardContext> shardContexts) {
        this.shardContexts = shardContexts;
    }

    public String getSubId() {
        return subId;
    }

    public void setSubId(String subId) {
        this.subId = subId;
    }
}