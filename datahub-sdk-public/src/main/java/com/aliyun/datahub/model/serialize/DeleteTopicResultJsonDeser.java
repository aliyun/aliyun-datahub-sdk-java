package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.DeleteTopicRequest;
import com.aliyun.datahub.model.DeleteTopicResult;
import com.aliyun.datahub.rest.DatahubHttpHeaders;

public class DeleteTopicResultJsonDeser implements Deserializer<DeleteTopicResult, DeleteTopicRequest, Response> {

    @Override
    public DeleteTopicResult deserialize(DeleteTopicRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        DeleteTopicResult rs = new DeleteTopicResult();
        rs.setRequestId(response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));
        return rs;
    }

    private static DeleteTopicResultJsonDeser instance;

    private DeleteTopicResultJsonDeser() {

    }

    public static DeleteTopicResultJsonDeser getInstance() {
        if (instance == null)
            instance = new DeleteTopicResultJsonDeser();
        return instance;
    }
}
