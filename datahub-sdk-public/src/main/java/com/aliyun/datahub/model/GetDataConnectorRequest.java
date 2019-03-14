package com.aliyun.datahub.model;

public class GetDataConnectorRequest {
    private String projectName;
    private String topicName;
    private ConnectorType type;

    public GetDataConnectorRequest(String projectName, String topicName, ConnectorType connectorType) {
        this.topicName = topicName;
        this.projectName = projectName;
        this.type = connectorType;
    }

    public String getTopicName() {
        return topicName;
    }

    public ConnectorType getConnectorType() {
        return type;
    }

    public String getProjectName() {
        return projectName;
    }
}
