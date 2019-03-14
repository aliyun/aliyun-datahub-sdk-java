package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.CreateProjectRequest;
import com.aliyun.datahub.model.CreateProjectResult;
import com.aliyun.datahub.rest.DatahubHttpHeaders;

public class CreateProjectResultJsonDeser implements Deserializer<CreateProjectResult, CreateProjectRequest, Response> {

    @Override
    public CreateProjectResult deserialize(CreateProjectRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        CreateProjectResult rs = new CreateProjectResult();
        rs.setRequestId(response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));
        return rs;
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
