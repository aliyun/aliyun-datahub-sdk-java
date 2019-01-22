package com.aliyun.datahub.model;

import com.aliyun.datahub.common.transport.HttpMethod;

public class GetTopicRequest {
    private String projectName;
    private String topicName;

    public GetTopicRequest(String projectName, String topicName) {
        this.projectName = projectName;
        this.topicName = topicName;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getProjectName() {
        return projectName;
    }
}
