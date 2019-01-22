package com.aliyun.datahub.model;


import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;

/**
 * 
 * Represents the input for <a>GetBlobRecords</a>.
 * 
 */
public class GetBlobRecordsRequest {
    private String projectName;
    private String topicName;
    private String shardId;
    /**
     *
     * The position in the shard from which you want to start sequentially
     * reading data records.
     *
     */
    private String cursor;
    /**
     *
     * The maximum number of records to return.
     *
     */
    private int limit;
    /**
     *
     * constructor
     *
     *
     * @param projectName The name of project.
     * @param topicName   The name of topic.
     * @param shardId     The shard ID of shard to get the cursor for.
     * @param cursor      The position in the shard from which you want to start sequentially
     *                    reading data records.
     */
    public GetBlobRecordsRequest(String projectName, String topicName, String shardId, String cursor, int limit) {
        this.projectName = projectName;
        this.topicName = topicName;
        this.shardId = shardId;
        this.cursor = cursor;
        this.limit = limit;
    }

    public String getProjectName() {
        return projectName;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getShardId() {
        return shardId;
    }

    public String getCursor() {
        return cursor;
    }

    public int getLimit() {
        return limit;
    }
}
