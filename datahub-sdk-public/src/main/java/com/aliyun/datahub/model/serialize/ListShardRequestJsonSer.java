package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.ListShardRequest;

public class ListShardRequestJsonSer implements Serializer<DefaultRequest, ListShardRequest> {
    @Override
    public DefaultRequest serialize(ListShardRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName() + "/topics/" + request.getTopicName() + "/shards");
        req.setHttpMethod(HttpMethod.GET);
        return req;
    }

    private ListShardRequestJsonSer() {

    }

    private static ListShardRequestJsonSer instance;

    public static ListShardRequestJsonSer getInstance() {
        if (instance == null)
            instance = new ListShardRequestJsonSer();
        return instance;
    }
}
