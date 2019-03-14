package com.aliyun.datahub.client.model;

public class SinkFcConfig extends SinkConfig {
    /**
     * The endpoint of the function compute service
     */
    private String endpoint;

    /**
     * The name of the function compute service
     */
    private String service;

    /**
     * The function name used to synchronize data
     */
    private String function;

    /**
     * Which mode used for user checking.
     */
    private AuthMode authMode;

    /**
     * If use AK mode, you should input accessId.
     */
    private String accessId;

    /**
     * If user AK mode, you should input accessKey.
     */
    private String accessKey;

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getFunction() {
        return function;
    }

    public void setFunction(String function) {
        this.function = function;
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
