package com.aliyun.datahub.model;

import com.aliyun.datahub.exception.InvalidParameterException;

public class UpdateTopicRequest {
    private String projectName;
    private String topicName;
    private int lifeCycle;
    private String comment;


    public String getComment() {
        return comment;
    }

    public String getProjectName() {
        return projectName;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getLifeCycle() {
        return lifeCycle;
    }

    public UpdateTopicRequest(String projectName, String topicName, int lifeCycle, String comment) {

        if (projectName == null ) {
            throw new InvalidParameterException("project name is null");
        }

        if (topicName == null) {
            throw new InvalidParameterException("topic name is null");
        }

        if (comment == null) {
            throw new InvalidParameterException("comment is null");
        }

        this.projectName = projectName;
        this.topicName = topicName;
        this.lifeCycle = lifeCycle;
        this.comment = comment;
    }
}
