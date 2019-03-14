package com.aliyun.datahub.exception;

public class NoPermissionException extends DatahubServiceException {
    public NoPermissionException(String msg) {
        super(msg);
    }
}
