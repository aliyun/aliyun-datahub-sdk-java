package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.ListConnectorRequest;
@Deprecated
public class ListConnectorRequestJsonSer implements Serializer<DefaultRequest, ListConnectorRequest> {
    @Override
    public DefaultRequest serialize(ListConnectorRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName()
                + "/topics/" + request.getTopicName()
                + "/connectors");
        req.setHttpMethod(HttpMethod.GET);

        return req;
    }

    private ListConnectorRequestJsonSer() {

    }

    private static ListConnectorRequestJsonSer instance;

    public static ListConnectorRequestJsonSer getInstance() {
        if (instance == null)
            instance = new ListConnectorRequestJsonSer();
        return instance;
    }
}
