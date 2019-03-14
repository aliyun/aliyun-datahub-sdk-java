package com.aliyun.datahub.client.exception;

public class AuthorizationFailureException extends DatahubClientException {
    public AuthorizationFailureException(DatahubClientException ex) {
        super(ex);
    }
}
