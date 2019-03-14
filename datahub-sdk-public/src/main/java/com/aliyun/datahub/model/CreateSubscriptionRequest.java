package com.aliyun.datahub.model;

import com.aliyun.datahub.exception.InvalidParameterException;

public class CreateSubscriptionRequest {
    private String projectName;
    private String topicName;
    private String comment;

    public CreateSubscriptionRequest(String project, String topic, String comment) {
        if (project == null) {
            throw new InvalidParameterException("project name is null");
        }

        if (topic == null) {
            throw new InvalidParameterException("topic name is null");
        }
        
        if (comment == null) {
        	comment = "";
        }

        this.projectName = project;
        this.topicName = topic;
        this.comment = comment;
    }

    public String getProjectName() {
        return projectName;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getComment() {
        return comment;
    }
}
