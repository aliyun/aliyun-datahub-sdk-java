package com.aliyun.datahub.client.exception;

public class ResourceAlreadyExistException extends DatahubClientException {
    public ResourceAlreadyExistException(DatahubClientException ex) {
        super(ex);
    }
}
