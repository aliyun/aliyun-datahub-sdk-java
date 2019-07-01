package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class RecordEntry {
    @JsonProperty("ShardId")
    private String shardId;

    @JsonProperty("PartitionKey")
    private String partitionKey;

    @JsonProperty("HashKey")
    private String hashKey;

    private long systemTime;

    private long sequence = -1;

    @JsonProperty("Cursor")
    private String cursor;

    @JsonProperty("NextCursor")
    private String nextCursor;

    @JsonProperty("Attributes")
    private Map<String, String> attributes;

    @JsonProperty("Data")
    private RecordData recordData;

    public void addAttribute(String key, String value) {
        if (attributes == null) {
            attributes = new HashMap<>();
        }
        attributes.put(key, value);
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

    @JsonIgnore
    public long getSystemTime() {
        return systemTime;
    }

    @JsonProperty("SystemTime")
    public void setSystemTime(long systemTime) {
        this.systemTime = systemTime;
    }

    @JsonIgnore
    public long getSequence() {
        return sequence;
    }

    @JsonProperty("Sequence")
    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    public String getCursor() {
        return cursor;
    }

    public void setCursor(String cursor) {
        this.cursor = cursor;
    }

    public String getNextCursor() {
        return nextCursor;
    }

    public void setNextCursor(String nextCursor) {
        this.nextCursor = nextCursor;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public RecordData getRecordData() {
        return recordData;
    }

    public void setRecordData(RecordData recordData) {
        this.recordData = recordData;
    }
}
