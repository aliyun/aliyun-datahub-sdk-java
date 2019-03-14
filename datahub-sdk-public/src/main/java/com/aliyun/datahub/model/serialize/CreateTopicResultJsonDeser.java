package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.CreateTopicRequest;
import com.aliyun.datahub.model.CreateTopicResult;
import com.aliyun.datahub.rest.DatahubHttpHeaders;

public class CreateTopicResultJsonDeser implements Deserializer<CreateTopicResult, CreateTopicRequest, Response> {

    @Override
    public CreateTopicResult deserialize(CreateTopicRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        CreateTopicResult rs = new CreateTopicResult();
        rs.setRequestId(response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));
        return rs;
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
