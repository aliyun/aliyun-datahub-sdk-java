package com.aliyun.datahub.model;
@Deprecated
public class GetConnectorRequest {
    private String projectName;
    private String topicName;
    private ConnectorType type;

    public GetConnectorRequest(String projectName, String topicName, ConnectorType connectorType) {
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
