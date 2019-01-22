package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.ExtendShardRequest;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;

public class ExtendShardRequestJsonSer implements Serializer<DefaultRequest, ExtendShardRequest> {
    @Override
    public DefaultRequest serialize(ExtendShardRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName() + "/topics/" + request.getTopicName() + "/shards");
        req.setHttpMethod(HttpMethod.POST);

        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("Action", "extend");
        node.put("ShardNumber", request.getShardNumber());
        if (request.getExtendMode() != null) {
            node.put("ExtendMode", request.getExtendMode());
        }
        try {
            req.setBody(mapper.writeValueAsString(node));
        } catch (IOException e) {
            throw new DatahubClientException("serialize error", e);
        }
        return req;
    }

    private ExtendShardRequestJsonSer() {

    }

    private static ExtendShardRequestJsonSer instance;

    public static ExtendShardRequestJsonSer getInstance() {
        if (instance == null)
            instance = new ExtendShardRequestJsonSer();
        return instance;
    }
}
