package com.aliyun.datahub.exception;

public class LimitExceededException extends DatahubServiceException {
    public LimitExceededException(String msg) {
        super(msg);
    }
}
