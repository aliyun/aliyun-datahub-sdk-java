package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.SplitShardRequest;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;

public class SplitShardRequestJsonSer implements Serializer<DefaultRequest, SplitShardRequest> {
    @Override
    public DefaultRequest serialize(SplitShardRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName() + "/topics/" + request.getTopicName() + "/shards");
        req.setHttpMethod(HttpMethod.POST);

        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("Action", "split");
        node.put("ShardId", request.getShardId());
        node.put("SplitKey", request.getSplitKey());
        try {
            req.setBody(mapper.writeValueAsString(node));
        } catch (IOException e) {
            throw new DatahubClientException("serialize error", e);
        }
        return req;
    }

    private SplitShardRequestJsonSer() {

    }

    private static SplitShardRequestJsonSer instance;

    public static SplitShardRequestJsonSer getInstance() {
        if (instance == null)
            instance = new SplitShardRequestJsonSer();
        return instance;
    }
}
