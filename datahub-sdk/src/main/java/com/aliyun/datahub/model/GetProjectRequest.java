package com.aliyun.datahub.model;

import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.InvalidParameterException;

public class GetProjectRequest {
    private String projectName;

    public GetProjectRequest(String projectName) {
        if (projectName == null) {
            throw new InvalidParameterException("project name is null");
        }

        this.projectName = projectName;
    }

    public String getProjectName() {
        return this.projectName;
    }
}

