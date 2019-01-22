package com.aliyun.datahub.model;

import com.aliyun.datahub.DatahubConstants;
import com.aliyun.datahub.common.util.JacksonParser;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.util.HashMap;
import java.util.Map;

public abstract class Record {
    private String partitionKey;
    private String hashKey;
    private String shardId;
    private Map<String, String> attributes = new HashMap<String, String>();
    private long systemTime;
    private long sequence;

    public Record() {}

    abstract public long getRecordSize();

    abstract public JsonNode toJsonNode();

    abstract public void clear();


    public long getSystemTime() {
        return systemTime;
    }

    public void setSystemTime(long systemTime) {
        this.systemTime = systemTime;
    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public String getHashKey() {
        return hashKey;
    }

    public void setHashKey(String hashKey) {
        this.hashKey = hashKey;
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }
    
    public OffsetContext.Offset getOffset() {
    	return new OffsetContext.Offset(sequence, systemTime);
    }

    public Map<String, String> getAttributes() {
        return new HashMap<String, String>(attributes);
    }

    public void putAttribute(String key, String value) {
        attributes.put(key, value);
    }

    protected ObjectNode toObjectNode() {
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        if (shardId != null && !shardId.isEmpty()) {
            node.put(DatahubConstants.ShardId, shardId);
        } else if (partitionKey != null && !partitionKey.isEmpty()) {
            node.put(DatahubConstants.PartitionKey, partitionKey);
        } else if (hashKey != null && !hashKey.isEmpty()) {
            node.put(DatahubConstants.HashKey, hashKey);
        }

        ObjectNode attr = node.putObject("Attributes");
        for (String key : attributes.keySet()) {
            attr.put(key, attributes.get(key));
        }
        return node;
    }
}
