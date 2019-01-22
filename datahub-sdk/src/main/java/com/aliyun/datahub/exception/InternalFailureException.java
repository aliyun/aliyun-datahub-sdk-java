package com.aliyun.datahub.exception;


public class InternalFailureException extends DatahubServiceException {
    public InternalFailureException(String errorMessage) {
        super(errorMessage);
    }
}
