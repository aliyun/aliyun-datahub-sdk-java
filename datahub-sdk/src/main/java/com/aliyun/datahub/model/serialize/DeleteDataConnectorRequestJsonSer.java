package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.DeleteDataConnectorRequest;

public class DeleteDataConnectorRequestJsonSer implements Serializer<DefaultRequest, DeleteDataConnectorRequest> {
    @Override
    public DefaultRequest serialize(DeleteDataConnectorRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName()
                + "/topics/" + request.getTopicName()
                + "/connectors/" + request.getConnectorType().toString().toLowerCase());
        req.setHttpMethod(HttpMethod.DELETE);
        return req;
    }

    private DeleteDataConnectorRequestJsonSer() {

    }

    private static DeleteDataConnectorRequestJsonSer instance;

    public static DeleteDataConnectorRequestJsonSer getInstance() {
        if (instance == null)
            instance = new DeleteDataConnectorRequestJsonSer();
        return instance;
    }
}
