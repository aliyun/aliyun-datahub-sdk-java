package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.ReloadConnectorRequest;
import com.aliyun.datahub.model.ReloadConnectorResult;
import com.aliyun.datahub.rest.DatahubHttpHeaders;

@Deprecated
public class ReloadConnectorResultJsonDeser implements Deserializer<ReloadConnectorResult,ReloadConnectorRequest,Response> {
    @Override
    public ReloadConnectorResult deserialize(ReloadConnectorRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        ReloadConnectorResult rs = new ReloadConnectorResult();
        rs.setRequestId(response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));
        return rs;
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
