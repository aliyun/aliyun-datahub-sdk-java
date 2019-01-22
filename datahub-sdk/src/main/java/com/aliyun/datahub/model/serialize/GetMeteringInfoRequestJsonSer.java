package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.GetMeteringInfoRequest;

public class GetMeteringInfoRequestJsonSer implements Serializer<DefaultRequest, GetMeteringInfoRequest> {

    @Override
    public DefaultRequest serialize(GetMeteringInfoRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();

        req.setHttpMethod(HttpMethod.POST);
        req.setResource("/projects/" + request.getProjectName() + "/topics/"
                + request.getTopicName() + "/shards/" + request.getShardId());

        String body = "{\"Action\":\"meter\"}";
        req.setBody(body);
        return req;
    }

    private GetMeteringInfoRequestJsonSer() {

    }

    private static GetMeteringInfoRequestJsonSer instance;

    public static GetMeteringInfoRequestJsonSer getInstance() {
        if (instance == null)
            instance = new GetMeteringInfoRequestJsonSer();
        return instance;
    }
}
