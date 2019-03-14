package com.aliyun.datahub.client.model;

public class SinkOtsConfig extends SinkConfig {
    /**
     * Endpoint of the ots service
     */
    private String endpoint;

    /**
     * Instance name of the ots service
     */
    private String instance;

    /**
     * table name of the ots service
     */
    private String table;

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

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
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
