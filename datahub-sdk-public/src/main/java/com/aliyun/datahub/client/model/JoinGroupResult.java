package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JoinGroupResult extends BaseResult {
    @JsonProperty("ConsumerId")
    private String consumerId;

    @JsonProperty("VersionId")
    private long versionId;

    @JsonProperty("SessionTimeout")
    private long sessionTimeout;

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public long getVersionId() {
        return versionId;
    }

    public void setVersionId(long versionId) {
        this.versionId = versionId;
    }

    public long getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(long sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }
}
