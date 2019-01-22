package com.aliyun.datahub.model;

import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.InvalidParameterException;

public class DeleteProjectRequest {
    private String projectName;

    public String getProjectName() { return projectName; }

    public DeleteProjectRequest(String projectName) {
        if (projectName == null) {
            throw new InvalidParameterException("project name is null");
        }

        this.projectName = projectName;
    }
}
