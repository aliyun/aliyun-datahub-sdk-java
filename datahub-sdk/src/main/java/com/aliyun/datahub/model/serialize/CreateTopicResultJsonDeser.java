package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.CreateProjectResult;
import com.aliyun.datahub.model.CreateTopicRequest;
import com.aliyun.datahub.model.CreateTopicResult;

public class CreateTopicResultJsonDeser implements Deserializer<CreateTopicResult, CreateTopicRequest, Response> {

    @Override
    public CreateTopicResult deserialize(CreateTopicRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }
        return new CreateTopicResult();
    }

    private static CreateTopicResultJsonDeser instance;

    private CreateTopicResultJsonDeser() {

    }

    public static CreateTopicResultJsonDeser getInstance() {
        if (instance == null)
            instance = new CreateTopicResultJsonDeser();
        return instance;
    }
}
