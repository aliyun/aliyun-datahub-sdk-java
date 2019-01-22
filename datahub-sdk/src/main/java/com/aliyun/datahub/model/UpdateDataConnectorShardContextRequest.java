package com.aliyun.datahub.model;

import com.aliyun.datahub.exception.InvalidParameterException;

public class UpdateDataConnectorShardContextRequest {
    private String projectName;
    private String topicName;
    private ConnectorType connectorType;
    private ShardContext shardContext;

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public ConnectorType getConnectorType() {
        return connectorType;
    }

    public void setConnectorType(ConnectorType connectorType) {
        this.connectorType = connectorType;
    }

    public ShardContext getShardContext() {
        return shardContext;
    }

    public void setShardContext(ShardContext shardContext) {
        this.shardContext = shardContext;
    }

    public UpdateDataConnectorShardContextRequest(String projectName, String topicName,
                                                  ConnectorType connectorType, ShardContext shardContext) {
        super();

        if (projectName == null) {
            throw new InvalidParameterException("project name is null");
        }

        if (topicName == null) {
            throw new InvalidParameterException("topic name is null");
        }

        if (connectorType == null) {
            throw new InvalidParameterException("connectorType is null");
        }

        if (shardContext == null) {
            throw new InvalidParameterException("shardContext is null");
        }

        this.projectName = projectName;
        this.topicName = topicName;
        this.connectorType = connectorType;
        this.shardContext = shardContext;
    }
}
