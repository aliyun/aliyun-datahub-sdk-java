package com.aliyun.datahub.model;

import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.InvalidParameterException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public class CreateTopicRequest {
    private String projectName;
    private String topicName;
    private int shardCount;
    private int lifeCycle;
    private RecordType recordType;
    private RecordSchema recordSchema;
    private String comment;

    public String getProjectName() {
        return projectName;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getShardCount() {
        return shardCount;
    }

    public int getLifeCycle() {
        return lifeCycle;
    }

    public RecordType getRecordType() {
        return recordType;
    }

    public RecordSchema getRecordSchema() {
        return recordSchema;
    }

    public String getComment() {
        return comment;
    }

    public CreateTopicRequest(String projectName, String topicName, int shardCount, int lifeCycle, RecordType recordType, RecordSchema recordSchema, String comment) {
        super();

        if (projectName == null) {
            throw new InvalidParameterException("project name is null");
        }

        if (topicName == null) {
            throw new InvalidParameterException("topic name is null");
        }

        if (recordType == null) {
            throw new InvalidParameterException("record type is null");
        }

        if (recordType == RecordType.TUPLE && recordSchema == null) {
            throw new InvalidParameterException("record schema is null for TUPLE topic");
        }

        if (recordType == RecordType.BLOB && recordSchema != null) {
            throw new InvalidParameterException("record schema must be null for BLOB topic");
        }

        if (comment == null) {
            throw new InvalidParameterException("comment jis null");
        }

        this.projectName = projectName;
        this.topicName = topicName;
        this.shardCount = shardCount;
        this.lifeCycle = lifeCycle;
        this.recordType = recordType;
        this.recordSchema = recordSchema;
        this.comment = comment;
    }
}
