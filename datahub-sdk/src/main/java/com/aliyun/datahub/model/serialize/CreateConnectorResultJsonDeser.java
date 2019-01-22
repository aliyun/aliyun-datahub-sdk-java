package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.CreateConnectorRequest;
import com.aliyun.datahub.model.CreateConnectorResult;

@Deprecated
public class CreateConnectorResultJsonDeser implements Deserializer<CreateConnectorResult,CreateConnectorRequest,Response> {
    @Override
    public CreateConnectorResult deserialize(CreateConnectorRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }
        return new CreateConnectorResult();
    }

    private CreateConnectorResultJsonDeser() {

    }

    private static CreateConnectorResultJsonDeser instance;

    public static CreateConnectorResultJsonDeser getInstance() {
        if (instance == null)
            instance = new CreateConnectorResultJsonDeser();
        return instance;
    }
}
