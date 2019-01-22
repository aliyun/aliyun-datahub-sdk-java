package com.aliyun.datahub.model;

import com.aliyun.datahub.common.transport.HttpMethod;

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

