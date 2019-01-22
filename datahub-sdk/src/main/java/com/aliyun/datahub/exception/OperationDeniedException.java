package com.aliyun.datahub.exception;

public class OperationDeniedException extends DatahubServiceException {
    public OperationDeniedException(String msg) {
        super(msg);
    }
}
