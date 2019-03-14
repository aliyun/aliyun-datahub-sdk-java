package com.aliyun.datahub.model;

import com.aliyun.datahub.common.data.SubscriptionState;
import com.aliyun.datahub.exception.InvalidParameterException;

public class UpdateSubscriptionStateRequest {
    private String projectName;
    private String topicName;
    private String subId;
    private SubscriptionState state;

    public UpdateSubscriptionStateRequest(String project, String topic, String subId, SubscriptionState state) {
        if (project == null) {
            throw new InvalidParameterException("project name is null");
        }
        
        if (topic == null) {
        	throw new InvalidParameterException("topic name is null");
        }

        if (subId == null) {
            throw new InvalidParameterException("sub id is null");
        }

        if (state == null) {
            throw new InvalidParameterException("state is null");
        }

        this.projectName = project;
        this.topicName = topic;
        this.subId = subId;
        this.state = state;
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

    public SubscriptionState getState() {
        return state;
    }
}

