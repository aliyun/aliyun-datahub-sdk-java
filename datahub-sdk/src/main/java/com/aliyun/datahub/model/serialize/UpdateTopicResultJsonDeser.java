package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.UpdateTopicRequest;
import com.aliyun.datahub.model.UpdateTopicResult;

public class UpdateTopicResultJsonDeser implements Deserializer<UpdateTopicResult,UpdateTopicRequest,Response> {
    @Override
    public UpdateTopicResult deserialize(UpdateTopicRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }
        return new UpdateTopicResult();
    }
    private UpdateTopicResultJsonDeser() {

    }

    private static UpdateTopicResultJsonDeser instance;

    public static UpdateTopicResultJsonDeser getInstance() {
        if (instance == null)
            instance = new UpdateTopicResultJsonDeser();
        return instance;
    }
}
