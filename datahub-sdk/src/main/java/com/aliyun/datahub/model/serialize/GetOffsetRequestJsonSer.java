package com.aliyun.datahub.model.serialize;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.GetOffsetRequest;

public class GetOffsetRequestJsonSer implements Serializer<DefaultRequest, GetOffsetRequest> {
    @Override
    public DefaultRequest serialize(GetOffsetRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName() + "/topics/" + request.getTopicName() + "/subscriptions/" + request.getSubId() + "/offsets");
        req.setHttpMethod(HttpMethod.POST);
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        try {
            ObjectNode node = mapper.createObjectNode();
            node.put("Action", "get");
            ArrayNode shardIds = mapper.createArrayNode();
            if (request.getShardIds() != null) {
                for (String i : request.getShardIds()) {
                    shardIds.add(i);
                }
            }
            node.put("ShardIds", shardIds);
			req.setBody(mapper.writeValueAsString(node));
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        return req;
    }

    private GetOffsetRequestJsonSer() {}

    private static GetOffsetRequestJsonSer instance;

    public static GetOffsetRequestJsonSer getInstance() {
        if (instance == null) {
            instance = new GetOffsetRequestJsonSer();
        }
        return instance;
    }
}
