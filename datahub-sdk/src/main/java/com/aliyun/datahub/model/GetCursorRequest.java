package com.aliyun.datahub.model;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;

/**
 * 
 * Represents the input for <code>GetCursor</code>.
 * 
 */
public class GetCursorRequest {

    /**
     * 
     * Determines how the shard cursor is used to start reading data records
     * from the shard.
     * 
     */
    public enum CursorType {

        /**
         * Start reading at the last untrimmed record in the
         * shard in the system, which is the oldest data record in the shard.
         */
        OLDEST,
        /**
         * Start reading just after the most recent record in the
         * shard, so that you always read the most recent data in the shard.
         */
        LATEST,
        /**
         * Get cursor by timestamp mode
         */
        SYSTEM_TIME,
        /**
         * Get cursor by sequence mode
         */
        SEQUENCE,
    }

    private String projectName;
    private String topicName;
    private String shardId;
    private long timestamp;
    private long sequence;
    private CursorType type;

    /**
     * 
     * constructor
     * 
     *
     * @param projectName The name of project.
     * @param topicName   The name of topic.
     * @param shardId     The shard ID of shard to get the cursor for.
     * @param timestamp   Start reading records that received at the most recent time after 'timestamp',
     *                    represents as unix epoch in milliseconds.
     */
    public GetCursorRequest(String projectName, String topicName, String shardId, long timestamp) {
        this.projectName = projectName;
        this.topicName = topicName;
        this.shardId = shardId;
        this.timestamp = timestamp;
    }

    /**
     *
     * constructor
     *
     *
     * @param projectName The name of project.
     * @param topicName   The name of topic.
     * @param shardId     The shard ID of shard to get the cursor for.
     * @param type        The type of cursor
     * @param param       The relative param when get cursor, which is timestamp or sequence
     */

    public GetCursorRequest(String projectName, String topicName, String shardId, CursorType type, long param) {
        this.projectName = projectName;
        this.topicName = topicName;
        this.shardId = shardId;
        this.type = type;
        if (type == CursorType.SYSTEM_TIME) {
            this.timestamp = param;
        }
        else {
            this.sequence = param;
        }
    }

    /**
     * 
     * constructor
     * 
     *
     * @param projectName The name of project.
     * @param topicName   The name of topic.
     * @param shardId     The shard ID of shard to get the cursor for.
     * @param type        The type of cursor
     */
    public GetCursorRequest(String projectName, String topicName, String shardId, CursorType type) {
        this.projectName = projectName;
        this.topicName = topicName;
        this.shardId = shardId;
        this.type = type;
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

    public long getTimestamp() {
        return timestamp;
    }

    public long getSequence() {
        return sequence;
    }

    public CursorType getType() {
        return type;
    }
}
