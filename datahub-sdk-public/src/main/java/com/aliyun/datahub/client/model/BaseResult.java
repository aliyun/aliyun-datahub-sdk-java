package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

public abstract class BaseResult {
    @JsonIgnore
    private String requestId;

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }
}
