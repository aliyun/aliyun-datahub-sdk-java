package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.ListTopicRequest;
import com.aliyun.datahub.model.ListTopicResult;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;

public class ListTopicResultJsonDeser implements Deserializer<ListTopicResult,ListTopicRequest,Response> {
    @Override
    public ListTopicResult deserialize(ListTopicRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        ListTopicResult rs = new ListTopicResult();
        ByteArrayInputStream is = new ByteArrayInputStream(response.getBody());
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        JsonNode tree = null;
        try {
            tree = mapper.readTree(is);
        } catch (IOException e) {
            throw new DatahubServiceException(
                    "JsonParseError", "Parse body failed:" + response.getBody(), response);
        }
        JsonNode node = tree.get("TopicNames");
        if (node != null && !node.isNull()) {
            if (node.isArray()) {
                Iterator<JsonNode> it = node.getElements();
                while (it.hasNext()) {
                    JsonNode topic = it.next();
                    rs.addTopic(topic.asText());
                }
            }
        }
        return rs;
    }

    private ListTopicResultJsonDeser() {

    }

    private static ListTopicResultJsonDeser instance;

    public static ListTopicResultJsonDeser getInstance() {
        if (instance == null)
            instance = new ListTopicResultJsonDeser();
        return instance;
    }
}
