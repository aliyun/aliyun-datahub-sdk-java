package com.aliyun.datahub.model;

import com.aliyun.datahub.exception.InvalidParameterException;

public class UpdateDataConnectorStateRequest {
    private String projectName;
    private String topicName;
    private ConnectorType connectorType;
    private ConnectorState connectorState;

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

    public ConnectorState getConnectorState() {
        return connectorState;
    }

    public void setConnectorState(ConnectorState connectorState) {
        this.connectorState = connectorState;
    }

    public UpdateDataConnectorStateRequest(String projectName, String topicName,
                                           ConnectorType connectorType, ConnectorState connectorState) {
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

        if (connectorState == null) {
            throw new InvalidParameterException("connectorState is null");
        }

        this.projectName = projectName;
        this.topicName = topicName;
        this.connectorType = connectorType;
        this.connectorState = connectorState;
    }
}
