package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.DeleteConnectorRequest;
@Deprecated
public class DeleteConnectorRequestJsonSer implements Serializer<DefaultRequest, DeleteConnectorRequest> {
    @Override
    public DefaultRequest serialize(DeleteConnectorRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName()
                + "/topics/" + request.getTopicName()
                + "/connectors/" + request.getConnectorType().toString().toLowerCase());
        req.setHttpMethod(HttpMethod.DELETE);
        return req;
    }

    private DeleteConnectorRequestJsonSer() {

    }

    private static DeleteConnectorRequestJsonSer instance;

    public static DeleteConnectorRequestJsonSer getInstance() {
        if (instance == null)
            instance = new DeleteConnectorRequestJsonSer();
        return instance;
    }
}
