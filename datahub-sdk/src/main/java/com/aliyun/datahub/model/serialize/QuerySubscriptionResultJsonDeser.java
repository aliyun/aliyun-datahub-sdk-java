package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.GetSubscriptionResult;
import com.aliyun.datahub.model.QuerySubscriptionRequest;
import com.aliyun.datahub.model.QuerySubscriptionResult;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QuerySubscriptionResultJsonDeser implements Deserializer<QuerySubscriptionResult, QuerySubscriptionRequest, Response> {
    @Override
    public QuerySubscriptionResult deserialize(QuerySubscriptionRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        QuerySubscriptionResult rs = new QuerySubscriptionResult();
        ObjectMapper mapper = JacksonParser.getObjectMapper();

        JsonNode tree = null;
        try {
            tree = mapper.readTree(response.getBody());
        } catch (IOException e) {
            throw new DatahubServiceException(
                    "JsonParseError", "Parse body failed:" + response.getBody(), response);
        }

        rs.setTotalCount(tree.get("TotalCount").asLong());

        JsonNode subscriptionsNode = tree.get("Subscriptions");
        List<GetSubscriptionResult> subscriptions = new ArrayList<GetSubscriptionResult>();
        Iterator<JsonNode> itSub = subscriptionsNode.getElements();
        while (itSub.hasNext()) {
            JsonNode subNode = itSub.next();
        	GetSubscriptionResult sub = new GetSubscriptionResult();
            sub.setIsOwner(subNode.get("IsOwner").asBoolean());
            sub.setSubId(subNode.get("SubId").asText());
            sub.setTopicName(subNode.get("TopicName").asText());
            sub.setComment(subNode.get("Comment").asText());
            sub.setType(subNode.get("Type").asInt());
            sub.setState(subNode.get("State").asInt());
            sub.setCreateTime(subNode.get("CreateTime").asLong() * 1000);
            sub.setLastModifyTime(subNode.get("LastModifyTime").asLong() * 1000);
            subscriptions.add(sub);
        }
        rs.setSubscriptions(subscriptions);

        return rs;
    }

    private QuerySubscriptionResultJsonDeser() {}

    private static QuerySubscriptionResultJsonDeser instance;

    public static QuerySubscriptionResultJsonDeser getInstance() {
        if (instance == null) {
            instance = new QuerySubscriptionResultJsonDeser();
        }
        return instance;
    }
}
