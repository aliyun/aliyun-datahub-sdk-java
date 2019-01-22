package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.AppendDataConnectorFieldRequest;
import com.aliyun.datahub.model.AppendDataConnectorFieldResult;


public class AppendDataConnectorFieldResultJsonDeser implements Deserializer<AppendDataConnectorFieldResult,AppendDataConnectorFieldRequest,Response> {
    @Override
    public AppendDataConnectorFieldResult deserialize(AppendDataConnectorFieldRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }
        return new AppendDataConnectorFieldResult();
    }

    private AppendDataConnectorFieldResultJsonDeser() {

    }

    private static AppendDataConnectorFieldResultJsonDeser instance;

    public static AppendDataConnectorFieldResultJsonDeser getInstance() {
        if (instance == null)
            instance = new AppendDataConnectorFieldResultJsonDeser();
        return instance;
    }
}
