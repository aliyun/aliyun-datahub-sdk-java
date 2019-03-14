package com.aliyun.datahub.model;

public class ListTopicRequest {
    private String projectName;

    public ListTopicRequest(String projectName) {
        this.projectName = projectName;
    }

    public String getProjectName() {
        return projectName;
    }
}
