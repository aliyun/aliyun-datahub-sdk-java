package com.aliyun.datahub.model;

public class ListShardRequest {
    private String projectName;
    private String topicName;

    public ListShardRequest(String projectName, String topicName) {
        this.projectName = projectName;
        this.topicName = topicName;
    }

    public String getProjectName() {
        return projectName;
    }

    public String getTopicName() {
        return topicName;
    }
}
