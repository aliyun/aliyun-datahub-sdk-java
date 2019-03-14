package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.DeleteProjectRequest;
import com.aliyun.datahub.model.DeleteProjectResult;
import com.aliyun.datahub.rest.DatahubHttpHeaders;

public class DeleteProjectResultJsonDeser implements Deserializer<DeleteProjectResult, DeleteProjectRequest, Response> {

    @Override
    public DeleteProjectResult deserialize(DeleteProjectRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        DeleteProjectResult rs = new DeleteProjectResult();
        rs.setRequestId(response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));
        return rs;
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
