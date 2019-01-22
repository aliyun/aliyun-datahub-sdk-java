package com.aliyun.datahub.model;

import java.util.Set;

import com.aliyun.datahub.exception.InvalidParameterException;

public class InitOffsetContextRequest {
    private String projectName;
    private String topicName;
    private String subId;
    private Set<String> shardIds;
    
    public InitOffsetContextRequest(String project, String topic, String subid, Set<String> shardIds) {
        if (project == null) {
            throw new InvalidParameterException("project name is null");
        }
        
        if (topic == null) {
        	throw new InvalidParameterException("topic name is null");
        }
        
        if (subid == null) {
        	throw new InvalidParameterException("subid is null");
        }
        
        if (shardIds == null || shardIds.isEmpty()) {
        	throw new InvalidParameterException("shard id set is empty");
        }

        this.projectName = project;
        this.topicName = topic;
        this.subId = subid;
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
