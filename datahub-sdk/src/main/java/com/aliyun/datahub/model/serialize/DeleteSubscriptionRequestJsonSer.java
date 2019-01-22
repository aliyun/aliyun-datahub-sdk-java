package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.DeleteSubscriptionRequest;

public class DeleteSubscriptionRequestJsonSer implements Serializer<DefaultRequest, DeleteSubscriptionRequest> {
    @Override
    public DefaultRequest serialize(DeleteSubscriptionRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setHttpMethod(HttpMethod.DELETE);
        req.setResource("/projects/" + request.getProjectName() + "/topics/" + request.getTopicName() + "/subscriptions/" + request.getSubId());
        return req;
    }

    private DeleteSubscriptionRequestJsonSer() {}

    private static DeleteSubscriptionRequestJsonSer instance;

    public static DeleteSubscriptionRequestJsonSer getInstance() {
        if (instance == null) {
            instance = new DeleteSubscriptionRequestJsonSer();
        }
        return instance;
    }
}
