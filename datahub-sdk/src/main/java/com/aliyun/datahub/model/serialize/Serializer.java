package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.GetCursorRequest;
import com.aliyun.datahub.model.GetCursorResult;

public interface Serializer<T, R> {
    public T serialize(R in) throws DatahubClientException;
}
