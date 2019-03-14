package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.CreateConnectorRequest;
import com.aliyun.datahub.model.CreateConnectorResult;
import com.aliyun.datahub.rest.DatahubHttpHeaders;

@Deprecated
public class CreateConnectorResultJsonDeser implements Deserializer<CreateConnectorResult,CreateConnectorRequest,Response> {
    @Override
    public CreateConnectorResult deserialize(CreateConnectorRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        CreateConnectorResult rs = new CreateConnectorResult();
        rs.setRequestId(response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));
        return rs;
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
