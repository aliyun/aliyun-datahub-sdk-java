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

    /**
     * Authentication mode when visiting oss service
     */
    private AuthMode authMode;

    /**
     * AccessId used to visit oss service
     */
    private String accessId;

    /**
     * AccessKey used to visit oss service
     */
    private String accessKey;

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

    public AuthMode getAuthMode() {
        return authMode;
    }

    public void setAuthMode(AuthMode authMode) {
        this.authMode = authMode;
    }

    public String getAccessId() {
        return accessId;
    }

    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }
}
