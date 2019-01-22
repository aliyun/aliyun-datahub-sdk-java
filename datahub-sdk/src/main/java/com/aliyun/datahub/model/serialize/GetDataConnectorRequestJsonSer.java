package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.GetDataConnectorRequest;

public class GetDataConnectorRequestJsonSer implements Serializer<DefaultRequest, GetDataConnectorRequest> {
    @Override
    public DefaultRequest serialize(GetDataConnectorRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName()
                + "/topics/" + request.getTopicName()
                + "/connectors/" + request.getConnectorType().toString().toLowerCase());
        req.setHttpMethod(HttpMethod.GET);
        return req;
    }

    private GetDataConnectorRequestJsonSer() {

    }

    private static GetDataConnectorRequestJsonSer instance;

    public static GetDataConnectorRequestJsonSer getInstance() {
        if (instance == null)
            instance = new GetDataConnectorRequestJsonSer();
        return instance;
    }
}
