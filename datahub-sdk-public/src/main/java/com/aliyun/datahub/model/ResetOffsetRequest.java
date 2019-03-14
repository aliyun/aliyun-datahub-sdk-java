package com.aliyun.datahub.model;

import java.security.InvalidParameterException;
import java.util.Map;

public class ResetOffsetRequest {
    private String projectName;
    private String topicName;
    private String subId;
    private Map<String, OffsetContext.Offset> offsets;

    public ResetOffsetRequest(String project, String topic, String subId, Map<String, OffsetContext.Offset> offsets) {
        if (project == null) {
            throw new InvalidParameterException("project name is null");
        }
        
        if (topic == null) {
        	throw new InvalidParameterException("topic name is null");
        }

        if (subId == null) {
            throw new InvalidParameterException("sub id is null");
        }

        if (offsets == null) {
            throw new InvalidParameterException("offsets is null");
        }

        this.projectName = project;
        this.topicName = topic;
        this.subId = subId;
        this.offsets = offsets;
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

    public Map<String, OffsetContext.Offset> getOffsets() {
        return offsets;
    }
}

