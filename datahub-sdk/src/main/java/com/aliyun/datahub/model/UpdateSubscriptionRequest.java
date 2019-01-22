package com.aliyun.datahub.model;

import com.aliyun.datahub.exception.InvalidParameterException;

public class UpdateSubscriptionRequest {
    private String projectName;
    private String topicName;
    private String subId;
    private String comment;

    public UpdateSubscriptionRequest(String project, String topic, String subId, String comment) {
        if (project == null) {
            throw new InvalidParameterException("project name is null");
        }
        
        if (topic == null) {
            throw new InvalidParameterException("topic name is null");
        }

        if (subId == null) {
            throw new InvalidParameterException("sub id is null");
        }

        if (comment == null) {
            throw new InvalidParameterException("comment is null");
        }

        this.projectName = project;
        this.topicName = topic;
        this.subId = subId;
        this.comment = comment;
    }

    public String getProjectName() {
        return projectName;
    }
    
    public String getTopicName() {
    	return topicName;
    }

    public String getSubId() {
        return subId;
    }

    public String getComment() {
        return comment;
    }
}
