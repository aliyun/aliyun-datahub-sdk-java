package com.aliyun.datahub.exception;

public class OffsetSessionClosedException extends DatahubServiceException {
    public OffsetSessionClosedException(String msg) {
        super(msg);
    }
}