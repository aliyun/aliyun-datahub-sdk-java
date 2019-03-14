package com.aliyun.datahub.model;

public abstract class Result {
    private String requestId;

    public Result() {}

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }
}
