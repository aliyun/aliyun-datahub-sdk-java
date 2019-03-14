package com.aliyun.datahub.model;

/**
 * 
 * Represents the input for <code>GetMeteringInfo</code>.
 * 
 */
public class GetMeteringInfoRequest {

    private String projectName;
    private String topicName;
    private String shardId;

    /**
     *
     * constructor
     *
     *
     * @param projectName The name of project.
     * @param topicName   The name of topic.
     * @param shardId     The shard ID of shard to get the cursor for.
     */
    public GetMeteringInfoRequest(String projectName, String topicName, String shardId) {
        this.projectName = projectName;
        this.topicName = topicName;
        this.shardId = shardId;
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
}
