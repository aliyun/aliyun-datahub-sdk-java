package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.ListTopicRequest;

public class ListTopicRequestJsonSer implements Serializer<DefaultRequest, ListTopicRequest> {
    @Override
    public DefaultRequest serialize(ListTopicRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName() + "/topics");
        req.setHttpMethod(HttpMethod.GET);
        return req;
    }

    private ListTopicRequestJsonSer() {

    }

    private static ListTopicRequestJsonSer instance;

    public static ListTopicRequestJsonSer getInstance() {
        if (instance == null)
            instance = new ListTopicRequestJsonSer();
        return instance;
    }
}
