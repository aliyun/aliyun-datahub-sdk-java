package com.aliyun.datahub.client.impl.request;

import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.RecordType;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CreateTopicRequest extends BaseRequest {

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

    public CreateTopicRequest() {
        setAction("create");
    }

    public int getShardCount() {
        return shardCount;
    }

    public CreateTopicRequest setShardCount(int shardCount) {
        this.shardCount = shardCount;
        return this;
    }

    public int getLifeCycle() {
        return lifeCycle;
    }

    public CreateTopicRequest setLifeCycle(int lifeCycle) {
        this.lifeCycle = lifeCycle;
        return this;
    }

    public RecordType getRecordType() {
        return recordType;
    }

    public CreateTopicRequest setRecordType(RecordType recordType) {
        this.recordType = recordType;
        return this;
    }

    public RecordSchema getRecordSchema() {
        return recordSchema;
    }

    public CreateTopicRequest setRecordSchema(RecordSchema recordSchema) {
        this.recordSchema = recordSchema;
        return this;
    }

    public String getComment() {
        return comment;
    }

    public CreateTopicRequest setComment(String comment) {
        this.comment = comment;
        return this;
    }
}
