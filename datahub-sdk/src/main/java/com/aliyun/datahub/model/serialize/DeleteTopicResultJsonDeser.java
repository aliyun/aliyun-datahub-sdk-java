package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.DeleteTopicRequest;
import com.aliyun.datahub.model.DeleteTopicResult;

public class DeleteTopicResultJsonDeser implements Deserializer<DeleteTopicResult, DeleteTopicRequest, Response> {

    @Override
    public DeleteTopicResult deserialize(DeleteTopicRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }
        return new DeleteTopicResult();
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
