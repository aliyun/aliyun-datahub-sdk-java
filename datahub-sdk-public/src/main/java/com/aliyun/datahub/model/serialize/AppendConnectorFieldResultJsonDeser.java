package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.AppendConnectorFieldRequest;
import com.aliyun.datahub.model.AppendConnectorFieldResult;
import com.aliyun.datahub.rest.DatahubHttpHeaders;

@Deprecated
public class AppendConnectorFieldResultJsonDeser implements Deserializer<AppendConnectorFieldResult,AppendConnectorFieldRequest,Response> {
    @Override
    public AppendConnectorFieldResult deserialize(AppendConnectorFieldRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        AppendConnectorFieldResult rs = new AppendConnectorFieldResult();
        rs.setRequestId(response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));
        return rs;
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
