package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.GetConnectorRequest;
@Deprecated
public class GetConnectorRequestJsonSer implements Serializer<DefaultRequest, GetConnectorRequest> {
    @Override
    public DefaultRequest serialize(GetConnectorRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName()
                + "/topics/" + request.getTopicName()
                + "/connectors/" + request.getConnectorType().toString().toLowerCase());
        req.setHttpMethod(HttpMethod.GET);
        return req;
    }

    private GetConnectorRequestJsonSer() {

    }

    private static GetConnectorRequestJsonSer instance;

    public static GetConnectorRequestJsonSer getInstance() {
        if (instance == null)
            instance = new GetConnectorRequestJsonSer();
        return instance;
    }
}
