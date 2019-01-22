package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.ReloadConnectorRequest;
import com.aliyun.datahub.model.ReloadConnectorResult;

@Deprecated
public class ReloadConnectorResultJsonDeser implements Deserializer<ReloadConnectorResult,ReloadConnectorRequest,Response> {
    @Override
    public ReloadConnectorResult deserialize(ReloadConnectorRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }
        return new ReloadConnectorResult();
    }

    private ReloadConnectorResultJsonDeser() {

    }

    private static ReloadConnectorResultJsonDeser instance;

    public static ReloadConnectorResultJsonDeser getInstance() {
        if (instance == null)
            instance = new ReloadConnectorResultJsonDeser();
        return instance;
    }
}
