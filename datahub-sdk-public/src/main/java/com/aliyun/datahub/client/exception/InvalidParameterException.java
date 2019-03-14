package com.aliyun.datahub.client.exception;

public class InvalidParameterException extends DatahubClientException {
    public InvalidParameterException(String message) {
        super(message);
    }

    public InvalidParameterException(DatahubClientException ex) {
        super(ex);
    }
}
