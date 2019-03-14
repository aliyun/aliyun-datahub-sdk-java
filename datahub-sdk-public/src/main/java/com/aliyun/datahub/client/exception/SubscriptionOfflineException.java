package com.aliyun.datahub.client.exception;

public class SubscriptionOfflineException extends DatahubClientException {
    public SubscriptionOfflineException(DatahubClientException ex) {
        super(ex);
    }
}
