package com.aliyun.datahub.model;

import com.aliyun.datahub.exception.InvalidParameterException;

public class QuerySubscriptionRequest {
    private String projectName;
    private String topicName;
    private String queryKey;
    private long pageIndex;
    private long pageSize;
    
    public QuerySubscriptionRequest(String project, String topic, String queryKey, long pageIndex, long pageSize) {
        if (project == null) {
            throw new InvalidParameterException("project name is null");
        }
        
        if (topic == null) {
        	throw new InvalidParameterException("topic name is null");
        }

        this.projectName = project;
        this.topicName = topic;
        this.queryKey = queryKey;
        this.pageIndex = pageIndex;
        this.pageSize = pageSize;
    }

    public String getProjectName() {
        return projectName;
    }

    public String getTopicName() {
        return topicName;
    }
    
    public String getQueryKey() {
    	return queryKey;
    }
    
    public long getPageIndex() {
    	return pageIndex;
    }
    
    public long getPageSize() {
    	return pageSize;
    }
}
