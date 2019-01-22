package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.GetTopicRequest;

public class GetTopicRequestJsonSer implements Serializer<DefaultRequest, GetTopicRequest> {

    @Override
    public DefaultRequest serialize(GetTopicRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setHttpMethod(HttpMethod.GET);
        req.setResource("/projects/" + request.getProjectName() + "/topics/" + request.getTopicName());
        return req;
    }

    private GetTopicRequestJsonSer() {

    }

    private static GetTopicRequestJsonSer instance;

    public static GetTopicRequestJsonSer getInstance() {
        if (instance == null)
            instance = new GetTopicRequestJsonSer();
        return instance;
    }
}
