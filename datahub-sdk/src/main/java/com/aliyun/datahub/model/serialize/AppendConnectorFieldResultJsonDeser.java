package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.AppendConnectorFieldRequest;
import com.aliyun.datahub.model.AppendConnectorFieldResult;

@Deprecated
public class AppendConnectorFieldResultJsonDeser implements Deserializer<AppendConnectorFieldResult,AppendConnectorFieldRequest,Response> {
    @Override
    public AppendConnectorFieldResult deserialize(AppendConnectorFieldRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }
        return new AppendConnectorFieldResult();
    }

    private AppendConnectorFieldResultJsonDeser() {

    }

    private static AppendConnectorFieldResultJsonDeser instance;

    public static AppendConnectorFieldResultJsonDeser getInstance() {
        if (instance == null)
            instance = new AppendConnectorFieldResultJsonDeser();
        return instance;
    }
}
