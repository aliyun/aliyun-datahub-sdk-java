package com.aliyun.datahub.client.exception;

public class NoPermissionException extends DatahubClientException {
    public NoPermissionException(String message) {
        super(message);
    }

    public NoPermissionException(DatahubClientException ex) {
        super(ex);
    }
}
