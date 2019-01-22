package com.aliyun.datahub.model;

import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.InvalidParameterException;

public class CreateProjectRequest {
    private String projectName;

    public String getComment() {
        return comment;
    }

    public String getProjectName() {
        return projectName;
    }

    private String comment;

    public CreateProjectRequest(String projectName, String comment) {
        if (projectName == null)  {
            throw new InvalidParameterException("project name is null");
        }

        if (comment == null)  {
            throw new InvalidParameterException("comment name is null");
        }

        this.projectName = projectName;
        this.comment = comment;
    }
}
