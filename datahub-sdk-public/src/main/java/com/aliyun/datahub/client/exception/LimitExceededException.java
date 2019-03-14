package com.aliyun.datahub.client.exception;

public class LimitExceededException extends DatahubClientException {
    public LimitExceededException(DatahubClientException ex) {
        super(ex);
    }
}
