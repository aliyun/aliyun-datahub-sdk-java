package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.DeleteTopicRequest;

public class DeleteTopicRequestJsonSer implements  Serializer<DefaultRequest, DeleteTopicRequest> {
    @Override
    public DefaultRequest serialize(DeleteTopicRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setHttpMethod(HttpMethod.DELETE);
        req.setResource("/projects/" + request.getProjectName() + "/topics/" + request.getTopicName());
        return req;
    }

    private DeleteTopicRequestJsonSer() {

    }

    private static DeleteTopicRequestJsonSer instance;

    public static DeleteTopicRequestJsonSer getInstance() {
        if (instance == null)
            instance = new DeleteTopicRequestJsonSer();
        return instance;
    }
}
