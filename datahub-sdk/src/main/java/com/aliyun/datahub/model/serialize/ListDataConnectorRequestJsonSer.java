package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.ListDataConnectorRequest;

public class ListDataConnectorRequestJsonSer implements Serializer<DefaultRequest, ListDataConnectorRequest> {
    @Override
    public DefaultRequest serialize(ListDataConnectorRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName()
                + "/topics/" + request.getTopicName()
                + "/connectors");
        req.setHttpMethod(HttpMethod.GET);

        return req;
    }

    private ListDataConnectorRequestJsonSer() {

    }

    private static ListDataConnectorRequestJsonSer instance;

    public static ListDataConnectorRequestJsonSer getInstance() {
        if (instance == null)
            instance = new ListDataConnectorRequestJsonSer();
        return instance;
    }
}
