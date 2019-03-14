package com.aliyun.datahub.exception;

import com.aliyun.datahub.common.transport.Response;

public class MalformedRecordException extends DatahubServiceException {
    public MalformedRecordException(String msg) {
        super(msg);
    }

    public MalformedRecordException(String msg, Response response) {
        super("MalformedRecord", msg, response);
    }
}
