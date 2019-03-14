package com.aliyun.datahub.client.exception;

public class MalformedRecordException extends DatahubClientException {
    public MalformedRecordException(String message) {
        super(message);
    }

    public MalformedRecordException(DatahubClientException ex) {
        super(ex);
    }

}
