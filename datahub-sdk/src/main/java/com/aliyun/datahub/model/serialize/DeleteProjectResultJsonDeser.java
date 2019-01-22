package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.DeleteProjectRequest;
import com.aliyun.datahub.model.DeleteProjectResult;

public class DeleteProjectResultJsonDeser implements Deserializer<DeleteProjectResult, DeleteProjectRequest, Response> {

    @Override
    public DeleteProjectResult deserialize(DeleteProjectRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }
        return new DeleteProjectResult();
    }

    private static DeleteProjectResultJsonDeser instance;

    private DeleteProjectResultJsonDeser() {

    }

    public static DeleteProjectResultJsonDeser getInstance() {
        if (instance == null)
            instance = new DeleteProjectResultJsonDeser();
        return instance;
    }
}
