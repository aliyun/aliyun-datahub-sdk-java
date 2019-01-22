package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.ListProjectRequest;

public class ListProjectRequestJsonSer implements Serializer<DefaultRequest, ListProjectRequest> {
    @Override
    public DefaultRequest serialize(ListProjectRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects");
        req.setHttpMethod(HttpMethod.GET);
        return req;
    }

    private ListProjectRequestJsonSer() {

    }

    private static ListProjectRequestJsonSer instance;

    public static ListProjectRequestJsonSer getInstance() {
        if (instance == null)
            instance = new ListProjectRequestJsonSer();
        return instance;
    }
}
