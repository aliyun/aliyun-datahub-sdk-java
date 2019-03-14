package com.aliyun.datahub.client.model;

public class SinkOssConfig extends SinkConfig {
    /**
     * Endpoint of the oss service
     */
    private String endpoint;

    /**
     * Bucket of the oss service
     */
    private String bucket;

    /**
     * Prefix of the oss service
     */
    private String prefix;

    /**
     * Specify how to compute partition
     */
    private String timeFormat;

    /**
     * How long time data that the oss partition to store.
     * <br>
     * Unit: Minute
     */
    private int timeRange;

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

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getTimeFormat() {
        return timeFormat;
    }

    public void setTimeFormat(String timeFormat) {
        this.timeFormat = timeFormat;
    }

    public int getTimeRange() {
        return timeRange;
    }

    public void setTimeRange(int timeRange) {
        this.timeRange = timeRange;
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
