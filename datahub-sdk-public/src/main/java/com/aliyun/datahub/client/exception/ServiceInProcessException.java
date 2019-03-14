package com.aliyun.datahub.client.exception;

public class ServiceInProcessException extends DatahubClientException {
    public ServiceInProcessException(DatahubClientException ex) {
        super(ex);
    }
}