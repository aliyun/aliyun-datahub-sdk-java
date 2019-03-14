package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.UpdateDataConnectorShardContextRequest;
import com.aliyun.datahub.model.UpdateDataConnectorShardContextResult;
import com.aliyun.datahub.rest.DatahubHttpHeaders;


public class UpdateDataConnectorShardContextResultJsonDeser implements Deserializer<UpdateDataConnectorShardContextResult,UpdateDataConnectorShardContextRequest,Response> {
    @Override
    public UpdateDataConnectorShardContextResult deserialize(UpdateDataConnectorShardContextRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        UpdateDataConnectorShardContextResult rs = new UpdateDataConnectorShardContextResult();
        rs.setRequestId(response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));
        return rs;
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
