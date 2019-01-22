package com.aliyun.datahub.exception;

public class AuthorizationFailureException extends DatahubServiceException {
    public AuthorizationFailureException(String msg) {
        super(msg);
    }
}
