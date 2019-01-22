package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.GetProjectRequest;

public class GetProjectRequestJsonSer implements Serializer<DefaultRequest, GetProjectRequest> {
    @Override
    public DefaultRequest serialize(GetProjectRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName());
        req.setHttpMethod(HttpMethod.GET);
        return req;
    }

    private GetProjectRequestJsonSer() {

    }

    private static GetProjectRequestJsonSer instance;

    public static GetProjectRequestJsonSer getInstance() {
        if (instance == null)
            instance = new GetProjectRequestJsonSer();
        return instance;
    }
}
