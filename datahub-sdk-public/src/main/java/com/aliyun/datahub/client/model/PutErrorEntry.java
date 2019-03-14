package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PutErrorEntry {
    @JsonProperty("Index")
    private int index;

    @JsonProperty("ErrorCode")
    private String errorcode;

    @JsonProperty("ErrorMessage")
    private String message;

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getErrorcode() {
        return errorcode;
    }

    public void setErrorcode(String errorcode) {
        this.errorcode = errorcode;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
