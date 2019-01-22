package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.GetSubscriptionRequest;

public class GetSubscriptionRequestJsonSer implements Serializer<DefaultRequest, GetSubscriptionRequest> {
    @Override
    public DefaultRequest serialize(GetSubscriptionRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setHttpMethod(HttpMethod.GET);
        req.setResource("/projects/" + request.getProjectName() + "/topics/" + request.getTopicName() + "/subscriptions/" + request.getSubId());
        return req;
    }

    private GetSubscriptionRequestJsonSer() {}

    private static GetSubscriptionRequestJsonSer instance;

    public static GetSubscriptionRequestJsonSer getInstance() {
        if (instance == null) {
            instance = new GetSubscriptionRequestJsonSer();
        }
        return instance;
    }
}
