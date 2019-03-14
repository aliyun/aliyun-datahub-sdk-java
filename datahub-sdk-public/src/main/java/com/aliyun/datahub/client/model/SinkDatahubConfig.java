package com.aliyun.datahub.client.model;


public class SinkDatahubConfig extends SinkConfig {
    /**
     * DataHub service endpoint
     */
    private String endpoint;

    /**
     * DataHub project name
     */
    private String projectName;

    /**
     * Datahub topic name
     */
    private String topicName;

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

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
}
