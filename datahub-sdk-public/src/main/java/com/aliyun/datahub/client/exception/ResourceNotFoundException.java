package com.aliyun.datahub.client.exception;

public class ResourceNotFoundException extends DatahubClientException {
    public ResourceNotFoundException(DatahubClientException ex) {
        super(ex);
    }
}
