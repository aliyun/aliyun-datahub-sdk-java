package com.aliyun.datahub.model;

public class ListDataConnectorRequest {
    private String topicName;
    private String projectName;
    public ListDataConnectorRequest(String projectName, String topicName) {
        this.topicName = topicName;
        this.projectName = projectName;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getProjectName() {
        return projectName;
    }
}
