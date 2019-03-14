package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.DeleteConnectorRequest;
import com.aliyun.datahub.model.DeleteConnectorResult;
import com.aliyun.datahub.rest.DatahubHttpHeaders;

@Deprecated
public class DeleteConnectorResultJsonDeser implements Deserializer<DeleteConnectorResult,DeleteConnectorRequest,Response> {
    @Override
    public DeleteConnectorResult deserialize(DeleteConnectorRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        DeleteConnectorResult rs = new DeleteConnectorResult();
        rs.setRequestId(response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));
        return rs;
    }

    private DeleteConnectorResultJsonDeser() {

    }

    private static DeleteConnectorResultJsonDeser instance;

    public static DeleteConnectorResultJsonDeser getInstance() {
        if (instance == null)
            instance = new DeleteConnectorResultJsonDeser();
        return instance;
    }
}
