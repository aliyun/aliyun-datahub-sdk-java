package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.data.SubscriptionState;
import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.UpdateSubscriptionStateRequest;

public class UpdateSubscriptionStateRequestJsonSer
		implements Serializer<DefaultRequest, UpdateSubscriptionStateRequest> {

    @Override
    public DefaultRequest serialize(UpdateSubscriptionStateRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setHttpMethod(HttpMethod.PUT);
        req.setResource("/projects/" + request.getProjectName() + "/topics/" + request.getTopicName() + "/subscriptions/" + request.getSubId());
        req.setBody("{\"State\": " + (request.getState() == SubscriptionState.INACTIVE ? 0 : 1) + "}");
        return req;
    }

    private UpdateSubscriptionStateRequestJsonSer() {}

    private static UpdateSubscriptionStateRequestJsonSer instance;

    public static UpdateSubscriptionStateRequestJsonSer getInstance() {
        if (instance == null) {
            instance = new UpdateSubscriptionStateRequestJsonSer();
        }
        return instance;
    }
}

