package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.CreateProjectRequest;
import com.aliyun.datahub.model.CreateProjectResult;

public class CreateProjectResultJsonDeser implements Deserializer<CreateProjectResult, CreateProjectRequest, Response> {

    @Override
    public CreateProjectResult deserialize(CreateProjectRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }
        return new CreateProjectResult();
    }

    private static CreateProjectResultJsonDeser instance;

    private CreateProjectResultJsonDeser() {

    }

    public static CreateProjectResultJsonDeser getInstance() {
        if (instance == null)
            instance = new CreateProjectResultJsonDeser();
        return instance;
    }
}
