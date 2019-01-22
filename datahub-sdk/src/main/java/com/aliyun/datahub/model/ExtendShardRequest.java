package com.aliyun.datahub.model;

public class ExtendShardRequest {
    private String projectName;
    private String topicName;
    private int shardNumber;
    private String extendMode;

    public ExtendShardRequest(String projectName, String topicName, int shardNumber) {
        this.projectName = projectName;
        this.topicName = topicName;
        this.shardNumber = shardNumber;
        this.extendMode = "inc";
    }

    public ExtendShardRequest(String projectName, String topicName, int shardNumber, String extendMode) {
        this.projectName = projectName;
        this.topicName = topicName;
        this.shardNumber = shardNumber;
        this.extendMode = extendMode;
    }

    public String getProjectName() {
        return projectName;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getShardNumber() {
        return shardNumber;
    }

    public String getExtendMode() {
        return extendMode;
    }
}
