package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubClientException;

public interface ErrorParser {
    public DatahubClientException parse(Response response);
}
