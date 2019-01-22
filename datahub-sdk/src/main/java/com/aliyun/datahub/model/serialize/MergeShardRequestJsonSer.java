package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.MergeShardRequest;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;

public class MergeShardRequestJsonSer implements Serializer<DefaultRequest, MergeShardRequest> {
    @Override
    public DefaultRequest serialize(MergeShardRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName() + "/topics/" + request.getTopicName() + "/shards");
        req.setHttpMethod(HttpMethod.POST);

        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("Action", "merge");
        node.put("ShardId", request.getShardId());
        node.put("AdjacentShardId", request.getAdjacentShardId());
        try {
            req.setBody(mapper.writeValueAsString(node));
        } catch (IOException e) {
            throw new DatahubClientException("serialize error", e);
        }
        return req;
    }

    private MergeShardRequestJsonSer() {

    }

    private static MergeShardRequestJsonSer instance;

    public static MergeShardRequestJsonSer getInstance() {
        if (instance == null)
            instance = new MergeShardRequestJsonSer();
        return instance;
    }
}
