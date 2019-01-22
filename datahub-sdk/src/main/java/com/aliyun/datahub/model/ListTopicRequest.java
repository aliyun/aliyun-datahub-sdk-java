package com.aliyun.datahub.model;

import com.aliyun.datahub.common.transport.HttpMethod;

public class ListTopicRequest {
    private String projectName;

    public ListTopicRequest(String projectName) {
        this.projectName = projectName;
    }

    public String getProjectName() {
        return projectName;
    }
}
