package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.UpdateDataConnectorShardContextRequest;
import com.aliyun.datahub.model.UpdateDataConnectorShardContextResult;


public class UpdateDataConnectorShardContextResultJsonDeser implements Deserializer<UpdateDataConnectorShardContextResult,UpdateDataConnectorShardContextRequest,Response> {
    @Override
    public UpdateDataConnectorShardContextResult deserialize(UpdateDataConnectorShardContextRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }
        return new UpdateDataConnectorShardContextResult();
    }

    private UpdateDataConnectorShardContextResultJsonDeser() {

    }

    private static UpdateDataConnectorShardContextResultJsonDeser instance;

    public static UpdateDataConnectorShardContextResultJsonDeser getInstance() {
        if (instance == null)
            instance = new UpdateDataConnectorShardContextResultJsonDeser();
        return instance;
    }
}
