package com.aliyun.datahub.model;
@Deprecated
public class ListConnectorRequest {
    private String topicName;
    private String projectName;
    public ListConnectorRequest(String projectName, String topicName) {
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
