package com.aliyun.datahub.client.exception;

public class SubscriptionOffsetResetException extends DatahubClientException {
    public SubscriptionOffsetResetException(DatahubClientException ex) {
        super(ex);
    }
}
