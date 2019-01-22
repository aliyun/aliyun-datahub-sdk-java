package com.aliyun.datahub.model;

import java.security.InvalidParameterException;
import java.util.Map;

public class CommitOffsetRequest {
	private String project;
	private String topic;
	private String subId;
    private Map<String, OffsetContext> offsetCtxMap;

    public CommitOffsetRequest(Map<String, OffsetContext> offsetCtxMap) {
        if (offsetCtxMap == null 
        		|| offsetCtxMap.isEmpty()) {
            throw new InvalidParameterException("offset context is null or empty");
        }

        this.offsetCtxMap = offsetCtxMap;
        OffsetContext offsetCtx = offsetCtxMap.values().iterator().next();
        this.project = offsetCtx.getProject();
        this.topic = offsetCtx.getTopic();
        this.subId = offsetCtx.getSubId();
    }

    public Map<String, OffsetContext> getOffsetCtxMap() {
        return offsetCtxMap;
    }
    
    public String getProject() {
    	return project;
    }
    
    public String getTopic() {
    	return topic;
    }
    
    public String getSubId() {
    	return subId;
    }
}
