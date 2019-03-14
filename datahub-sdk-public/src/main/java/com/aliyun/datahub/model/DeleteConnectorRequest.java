package com.aliyun.datahub.model;
@Deprecated
public class DeleteConnectorRequest {
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

    public DeleteConnectorRequest(String projectName, String topicName, ConnectorType connectorType) {
        this.topicName = topicName;
        this.projectName = projectName;
        this.connectorType = connectorType;
    }
}

