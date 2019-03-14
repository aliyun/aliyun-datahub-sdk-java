package com.aliyun.datahub.model;

public class SplitShardRequest {
    private String projectName;
    private String topicName;
    private String shardId;
    private String splitKey;

    public SplitShardRequest(String projectName, String topicName, String shardId, String splitKey) {
        this.projectName = projectName;
        this.topicName = topicName;
        this.shardId = shardId;
        this.splitKey = splitKey;
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

    public String getSplitKey() {
        return splitKey;
    }
}
