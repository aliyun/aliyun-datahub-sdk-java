package com.aliyun.datahub.model.serialize;


import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.CreateProjectRequest;

public class CreateProjectRequestJsonSer implements Serializer<DefaultRequest,CreateProjectRequest> {
    @Override
    public DefaultRequest serialize(CreateProjectRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setHttpMethod(HttpMethod.POST);
        req.setResource("/projects/" + request.getProjectName());
        req.setBody("{\"Comment\": \"" + request.getComment() + "\"}");
        return req;
    }

    private CreateProjectRequestJsonSer() {

    }

    private static CreateProjectRequestJsonSer instance;

    public static CreateProjectRequestJsonSer getInstance() {
        if (instance == null)
            instance = new CreateProjectRequestJsonSer();
        return instance;
    }
}
