package com.aliyun.datahub.model;

import com.aliyun.datahub.exception.InvalidParameterException;

public class DeleteSubscriptionRequest {
    private String projectName;
    private String topicName;
    private String subId;

    public DeleteSubscriptionRequest(String project, String topic, String subId) {
        if (project == null) {
            throw new InvalidParameterException("project name is null");
        }
        
        if (topic == null) {
        	throw new InvalidParameterException("topic name is null");
        }

        if (subId == null) {
            throw new InvalidParameterException("sub id is null");
        }

        this.projectName = project;
        this.topicName = topic;
        this.subId = subId;
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
}
