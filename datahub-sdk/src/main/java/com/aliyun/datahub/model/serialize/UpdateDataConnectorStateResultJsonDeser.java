package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.UpdateDataConnectorStateRequest;
import com.aliyun.datahub.model.UpdateDataConnectorStateResult;


public class UpdateDataConnectorStateResultJsonDeser implements Deserializer<UpdateDataConnectorStateResult,UpdateDataConnectorStateRequest,Response> {
    @Override
    public UpdateDataConnectorStateResult deserialize(UpdateDataConnectorStateRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }
        return new UpdateDataConnectorStateResult();
    }

    private UpdateDataConnectorStateResultJsonDeser() {

    }

    private static UpdateDataConnectorStateResultJsonDeser instance;

    public static UpdateDataConnectorStateResultJsonDeser getInstance() {
        if (instance == null)
            instance = new UpdateDataConnectorStateResultJsonDeser();
        return instance;
    }
}
