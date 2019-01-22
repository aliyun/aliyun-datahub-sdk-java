package com.aliyun.datahub.model;

import java.util.Set;

import com.aliyun.datahub.exception.InvalidParameterException;

public class GetOffsetRequest {
    private String projectName;
    private String topicName;
    private String subId;
    private Set<String> shardIds;

    public GetOffsetRequest(String project, String topic, String subId, Set<String> shardIds) {
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
        this.shardIds = shardIds;
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
    
    public Set<String> getShardIds() {
    	return shardIds;
    }
}
