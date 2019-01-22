package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.DeleteConnectorRequest;
import com.aliyun.datahub.model.DeleteConnectorResult;

@Deprecated
public class DeleteConnectorResultJsonDeser implements Deserializer<DeleteConnectorResult,DeleteConnectorRequest,Response> {
    @Override
    public DeleteConnectorResult deserialize(DeleteConnectorRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }
        return new DeleteConnectorResult();
    }

    private DeleteConnectorResultJsonDeser() {

    }

    private static DeleteConnectorResultJsonDeser instance;

    public static DeleteConnectorResultJsonDeser getInstance() {
        if (instance == null)
            instance = new DeleteConnectorResultJsonDeser();
        return instance;
    }
}
