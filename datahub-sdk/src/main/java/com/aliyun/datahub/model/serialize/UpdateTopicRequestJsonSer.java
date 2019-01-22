package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.model.UpdateTopicRequest;

public class UpdateTopicRequestJsonSer implements Serializer<DefaultRequest,UpdateTopicRequest> {
    @Override
    public DefaultRequest serialize(UpdateTopicRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setHttpMethod(HttpMethod.PUT);
        req.setResource("/projects/" + request.getProjectName() + "/topics/" + request.getTopicName());
        String body = "{\"" + "Lifecycle\":" + String.valueOf(request.getLifeCycle()) + "," +
                "\"Comment\":\"" + request.getComment()+ "\"}";
        req.setBody(body);
        return req;
    }
    private UpdateTopicRequestJsonSer() {

    }

    private static UpdateTopicRequestJsonSer instance;

    public static UpdateTopicRequestJsonSer getInstance() {
        if (instance == null)
            instance = new UpdateTopicRequestJsonSer();
        return instance;
    }
}
