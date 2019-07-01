package com.aliyun.datahub.client.exception;

public class SeekOutOfRangeException extends DatahubClientException {
    public SeekOutOfRangeException(DatahubClientException ex) {
        super(ex);
    }
}
