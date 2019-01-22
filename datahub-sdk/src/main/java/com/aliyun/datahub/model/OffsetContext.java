package com.aliyun.datahub.model;

import java.security.InvalidParameterException;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.aliyun.datahub.common.util.JacksonParser;

public class OffsetContext {
	/**
	 * Class Offset
	 *     具体点位信息值
	 * @author xushuai
	 * 
	 */
	public static class Offset {
		private long sequence;
	    private long timestamp;

	    public Offset(long seq, long ts) {
	        this.sequence = seq;
	        this.timestamp = ts;
	    }

	    public long getSequence() {
	        return sequence;
	    }

	    public void setSequence(long sequence) {
	        this.sequence = sequence;
	    }

	    public long getTimestamp() {
	        return timestamp;
	    }

	    public void setTimestamp(long timestamp) {
	        this.timestamp = timestamp;
	    }
	}
	
	private String project;
	private String topic;
	private String subId;
	private String shardId;
	private Offset offset;
	private long version;
	private String sessionId;
	
	public OffsetContext(String project, String topic, String subId, String shardId, Offset offset, long version, String sessionId) {
		if (project == null) {
            throw new InvalidParameterException("project name is null");
        }
        
        if (topic == null) {
        	throw new InvalidParameterException("topic name is null");
        }

        if (subId == null) {
            throw new InvalidParameterException("sub id is null");
        }
        
        if (shardId == null) {
        	throw new InvalidParameterException("shard id is null");
        }
        
        if (sessionId == null) {
        	throw new InvalidParameterException("session id is null");
        }
        
		this.project = project;
		this.topic = topic;
		this.subId = subId;
		this.shardId = shardId;
		this.offset = offset;
		this.version = version;
		this.sessionId = sessionId;
	}
	
	public void setOffset(Offset offset) {
		this.offset = offset;
	}

	public Offset getOffset() {
		return offset;
	}
	
	public void setVersion(long version) {
		this.version = version;
	}
	
	public long getVersion() {
		return version;
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
	
	public String getShardId() {
		return shardId;
	}
	
	public String getSessionId() {
		return sessionId;
	}
	
	public boolean hasOffset() {
		return offset == null ? false : offset.sequence < 0 ? false : offset.sequence != 0 || offset.timestamp != 0;
	}
	
	public ObjectNode toObjectNode() {
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("Project", project);
        node.put("Topic", topic);
        node.put("SubId", subId);
        node.put("ShardId", shardId);
        node.put("Sequence", offset.sequence);
        node.put("Timestamp", offset.timestamp);
        node.put("Version", version);
        node.put("SessionId", sessionId);
        return node;
    }
}
