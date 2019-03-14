package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.GetSubscriptionRequest;
import com.aliyun.datahub.model.GetSubscriptionResult;
import com.aliyun.datahub.rest.DatahubHttpHeaders;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class GetSubscriptionResultJsonDeser implements Deserializer <GetSubscriptionResult, GetSubscriptionRequest, Response>{
    @Override
    public GetSubscriptionResult deserialize(GetSubscriptionRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        GetSubscriptionResult rs = new GetSubscriptionResult();
        rs.setRequestId(response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));

        ObjectMapper mapper = JacksonParser.getObjectMapper();
        JsonNode tree = null;
        try {
            tree = mapper.readTree(response.getBody());
        } catch (IOException e) {
            throw new DatahubServiceException(
                    "JsonParseError", "Parse body failed:" + response.getBody(), response);
        }

        rs.setIsOwner(tree.get("IsOwner").asBoolean());
        rs.setSubId(tree.get("SubId").asText());
        rs.setTopicName(tree.get("TopicName").asText());
        rs.setComment(tree.get("Comment").asText());
        rs.setType(tree.get("Type").asInt());
        rs.setState(tree.get("State").asInt());
        rs.setCreateTime(tree.get("CreateTime").asLong() * 1000);
        rs.setLastModifyTime(tree.get("LastModifyTime").asLong() * 1000);
        return rs;
    }

    private GetSubscriptionResultJsonDeser() {}

    private static GetSubscriptionResultJsonDeser instance;

    public static GetSubscriptionResultJsonDeser getInstance() {
        if (instance == null) {
            instance = new GetSubscriptionResultJsonDeser();
        }
        return instance;
    }
}
