package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.DeleteDataConnectorRequest;
import com.aliyun.datahub.model.DeleteDataConnectorResult;


public class DeleteDataConnectorResultJsonDeser implements Deserializer<DeleteDataConnectorResult,DeleteDataConnectorRequest,Response> {
    @Override
    public DeleteDataConnectorResult deserialize(DeleteDataConnectorRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }
        return new DeleteDataConnectorResult();
    }

    private DeleteDataConnectorResultJsonDeser() {

    }

    private static DeleteDataConnectorResultJsonDeser instance;

    public static DeleteDataConnectorResultJsonDeser getInstance() {
        if (instance == null)
            instance = new DeleteDataConnectorResultJsonDeser();
        return instance;
    }
}
