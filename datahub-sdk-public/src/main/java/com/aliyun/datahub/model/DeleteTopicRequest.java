package com.aliyun.datahub.model;

public class DeleteTopicRequest {
    private String projectName;
    private String topicName;

    public String getProjectName() {
        return projectName;
    }

    public String getTopicName() {
        return topicName;
    }

    public DeleteTopicRequest(String projectName, String topicName) {
        this.projectName = projectName;
        this.topicName = topicName;
    }
}

