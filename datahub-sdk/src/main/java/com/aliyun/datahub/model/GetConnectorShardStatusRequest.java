package com.aliyun.datahub.model;

import com.aliyun.datahub.exception.InvalidParameterException;
@Deprecated
public class GetConnectorShardStatusRequest {
    private String projectName;
    private String topicName;
    private ConnectorType connectorType;
    private String shardId;

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

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    public GetConnectorShardStatusRequest(String projectName, String topicName,
                                          ConnectorType connectorType, String shardId) {
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

        if (shardId == null) {
            throw new InvalidParameterException("shardId is null");
        }

        this.projectName = projectName;
        this.topicName = topicName;
        this.connectorType = connectorType;
        this.shardId = shardId;
    }
}
