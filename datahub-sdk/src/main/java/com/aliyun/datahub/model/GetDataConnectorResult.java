package com.aliyun.datahub.model;

import java.util.ArrayList;
import java.util.List;

public class GetDataConnectorResult {
    private String state;
    private ConnectorType type;
    private String creator;
    private String owner;
    private List<String> columnFields;
    private ConnectorConfig config;
    private List<ShardContext> shardContexts;

    public GetDataConnectorResult(){
        this.columnFields = new ArrayList<String>();
        this.shardContexts = new ArrayList<ShardContext>();
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public List<ShardContext> getShardContexts() {
        return shardContexts;
    }

    public void setShardContext(List<ShardContext> shardContexts) {
        this.shardContexts = shardContexts;
    }

    public List<String> getColumnFields() {
        return columnFields;
    }

    public void setColumnFields(List<String> columnFields) {
        this.columnFields = columnFields;
    }

    public ConnectorConfig getConnectorConfig() {
        return config;
    }

    public void setConnectorConfig(ConnectorConfig config) {
        this.config = config;
    }

    public ConnectorType getType() {
        return type;
    }

    public void setType(String type) {
        this.type = ConnectorType.valueOf(type.toUpperCase());
    }
}
