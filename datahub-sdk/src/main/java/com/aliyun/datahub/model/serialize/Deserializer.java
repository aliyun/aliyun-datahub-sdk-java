package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.exception.DatahubServiceException;

public interface Deserializer<T, R1, R2> {
    public T deserialize(R1 request, R2 response) throws DatahubServiceException;
}
