package com.aliyun.datahub.model;

public class DeleteDataConnectorRequest {
    private String topicName;
    private String projectName;
    private ConnectorType connectorType;

    public String getTopicName() {
        return topicName;
    }

    public String getProjectName() {
        return projectName;
    }

    public ConnectorType getConnectorType() {
        return connectorType;
    }

    public DeleteDataConnectorRequest(String projectName, String topicName, ConnectorType connectorType) {
        this.topicName = topicName;
        this.projectName = projectName;
        this.connectorType = connectorType;
    }
}

