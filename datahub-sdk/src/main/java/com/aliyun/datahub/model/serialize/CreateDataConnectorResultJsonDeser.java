package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.CreateDataConnectorRequest;
import com.aliyun.datahub.model.CreateDataConnectorResult;


public class CreateDataConnectorResultJsonDeser implements Deserializer<CreateDataConnectorResult,CreateDataConnectorRequest,Response> {
    @Override
    public CreateDataConnectorResult deserialize(CreateDataConnectorRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }
        return new CreateDataConnectorResult();
    }

    private CreateDataConnectorResultJsonDeser() {

    }

    private static CreateDataConnectorResultJsonDeser instance;

    public static CreateDataConnectorResultJsonDeser getInstance() {
        if (instance == null)
            instance = new CreateDataConnectorResultJsonDeser();
        return instance;
    }
}
