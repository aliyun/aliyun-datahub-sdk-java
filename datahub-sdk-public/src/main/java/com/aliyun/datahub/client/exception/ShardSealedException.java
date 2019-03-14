package com.aliyun.datahub.client.exception;

public class ShardSealedException extends DatahubClientException {
    public ShardSealedException(DatahubClientException ex) {
        super(ex);
    }
}
