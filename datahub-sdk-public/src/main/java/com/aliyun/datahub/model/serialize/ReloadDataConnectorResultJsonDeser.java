package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.ReloadDataConnectorRequest;
import com.aliyun.datahub.model.ReloadDataConnectorResult;
import com.aliyun.datahub.rest.DatahubHttpHeaders;


public class ReloadDataConnectorResultJsonDeser implements Deserializer<ReloadDataConnectorResult,ReloadDataConnectorRequest,Response> {
    @Override
    public ReloadDataConnectorResult deserialize(ReloadDataConnectorRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        ReloadDataConnectorResult rs = new ReloadDataConnectorResult();
        rs.setRequestId(response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));
        return rs;
    }

    private ReloadDataConnectorResultJsonDeser() {

    }

    private static ReloadDataConnectorResultJsonDeser instance;

    public static ReloadDataConnectorResultJsonDeser getInstance() {
        if (instance == null)
            instance = new ReloadDataConnectorResultJsonDeser();
        return instance;
    }
}
