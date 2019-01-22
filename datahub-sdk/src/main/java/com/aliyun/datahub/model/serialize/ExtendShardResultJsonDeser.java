package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.ExtendShardResult;
import com.aliyun.datahub.model.ExtendShardRequest;


public class ExtendShardResultJsonDeser implements Deserializer<ExtendShardResult, ExtendShardRequest, Response> {

    @Override
    public ExtendShardResult deserialize(ExtendShardRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        return new ExtendShardResult();
    }

    private static ExtendShardResultJsonDeser instance;

    private ExtendShardResultJsonDeser() {

    }

    public static ExtendShardResultJsonDeser getInstance() {
        if (instance == null)
            instance = new ExtendShardResultJsonDeser();
        return instance;
    }
}
