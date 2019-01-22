package com.aliyun.datahub.model;

public class ErrorEntry {
    private String errorcode;
    private String message;

    public ErrorEntry(String errcode, String message) {
        this.errorcode = errcode;
        this.message = message;
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
