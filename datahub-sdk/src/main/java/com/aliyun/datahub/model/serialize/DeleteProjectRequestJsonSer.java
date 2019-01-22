package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.DeleteProjectRequest;

public class DeleteProjectRequestJsonSer implements Serializer<DefaultRequest, DeleteProjectRequest> {
    @Override
    public DefaultRequest serialize(DeleteProjectRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName());
        req.setHttpMethod(HttpMethod.DELETE);
        return req;
    }

    private DeleteProjectRequestJsonSer() {

    }

    private static DeleteProjectRequestJsonSer instance;

    public static DeleteProjectRequestJsonSer getInstance() {
        if (instance == null)
            instance = new DeleteProjectRequestJsonSer();
        return instance;
    }
}
