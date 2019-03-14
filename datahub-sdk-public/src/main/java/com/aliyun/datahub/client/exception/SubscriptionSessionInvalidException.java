package com.aliyun.datahub.client.exception;

public class SubscriptionSessionInvalidException extends DatahubClientException {
    public SubscriptionSessionInvalidException(DatahubClientException ex) {
        super(ex);
    }
}
