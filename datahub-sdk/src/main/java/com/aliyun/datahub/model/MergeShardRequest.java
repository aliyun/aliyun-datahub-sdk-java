package com.aliyun.datahub.model;

public class MergeShardRequest {
    private String projectName;
    private String topicName;
    private String shardId;
    private String adjacentShardId;

    public MergeShardRequest(String projectName, String topicName, String shardId, String adjacentShardId) {
        this.projectName = projectName;
        this.topicName = topicName;
        this.shardId = shardId;
        this.adjacentShardId = adjacentShardId;
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

    public String getAdjacentShardId() {
        return adjacentShardId;
    }
}
