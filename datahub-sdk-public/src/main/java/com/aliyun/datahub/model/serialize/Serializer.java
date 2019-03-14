package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.exception.DatahubClientException;

public interface Serializer<T, R> {
    public T serialize(R in) throws DatahubClientException;
}
