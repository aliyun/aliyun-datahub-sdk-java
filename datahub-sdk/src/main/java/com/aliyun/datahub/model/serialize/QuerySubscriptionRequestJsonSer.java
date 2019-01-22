package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.QuerySubscriptionRequest;

public class QuerySubscriptionRequestJsonSer implements Serializer<DefaultRequest, QuerySubscriptionRequest> {
    @Override
    public DefaultRequest serialize(QuerySubscriptionRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setHttpMethod(HttpMethod.POST);
        req.setResource("/projects/" + request.getProjectName() + "/topics/" + request.getTopicName() + "/subscriptions");
        String body = "{\"Action\":\"list\"";
        if (request.getQueryKey() != null) {
            body += ",\"Search\":\"" + request.getQueryKey() + "\"";
        }
        body += ",\"PageIndex\":" + request.getPageIndex() + ",\"PageSize\":" + request.getPageSize();
        body += "}";
        req.setBody(body);
        return req;
    }

    private QuerySubscriptionRequestJsonSer() {}

    private static QuerySubscriptionRequestJsonSer instance;

    public static QuerySubscriptionRequestJsonSer getInstance() {
        if (instance == null) {
            instance = new QuerySubscriptionRequestJsonSer();
        }
        return instance;
    }
}
