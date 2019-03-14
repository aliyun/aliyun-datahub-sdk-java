package com.aliyun.datahub.client.impl.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JoinGroupRequest extends BaseRequest {
    @JsonProperty("SessionTimeout")
    private long sessionTimeout;

    public JoinGroupRequest() {
        setAction("joinGroup");
    }

    public long getSessionTimeout() {
        return sessionTimeout;
    }

    public JoinGroupRequest setSessionTimeout(long sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
        return this;
    }
}
