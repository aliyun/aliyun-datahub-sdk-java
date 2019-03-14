package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GetTopicResult extends BaseResult {
    private String projectName;
    private String topicName;

    @JsonProperty("ShardCount")
    private int shardCount;

    @JsonProperty("Lifecycle")
    private int lifeCycle;

    @JsonProperty("RecordType")
    private RecordType recordType;

    @JsonProperty("RecordSchema")
    private RecordSchema recordSchema;

    @JsonProperty("Comment")
    private String comment;

    @JsonProperty("CreateTime")
    private long createTime;

    @JsonProperty("LastModifyTime")
    private long lastModifyTime;

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getShardCount() {
        return shardCount;
    }

    public void setShardCount(int shardCount) {
        this.shardCount = shardCount;
    }

    public int getLifeCycle() {
        return lifeCycle;
    }

    public void setLifeCycle(int lifeCycle) {
        this.lifeCycle = lifeCycle;
    }

    public RecordType getRecordType() {
        return recordType;
    }

    public void setRecordType(RecordType recordType) {
        this.recordType = recordType;
    }

    public RecordSchema getRecordSchema() {
        return recordSchema;
    }

    public void setRecordSchema(RecordSchema recordSchema) {
        this.recordSchema = recordSchema;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getLastModifyTime() {
        return lastModifyTime;
    }

    public void setLastModifyTime(long lastModifyTime) {
        this.lastModifyTime = lastModifyTime;
    }
}
