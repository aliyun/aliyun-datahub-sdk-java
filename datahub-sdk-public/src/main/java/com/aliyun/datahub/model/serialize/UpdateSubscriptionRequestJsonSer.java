package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.UpdateSubscriptionRequest;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

public class UpdateSubscriptionRequestJsonSer implements Serializer<DefaultRequest, UpdateSubscriptionRequest> {
    @Override
    public DefaultRequest serialize(UpdateSubscriptionRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setHttpMethod(HttpMethod.PUT);
        req.setResource("/projects/" + request.getProjectName() + "/topics/" + request.getTopicName() + "/subscriptions/" + request.getSubId());
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        try {
            ObjectNode node = mapper.createObjectNode();
            node.put("Comment", request.getComment());
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

    private UpdateSubscriptionRequestJsonSer() {}

    private static UpdateSubscriptionRequestJsonSer instance;

    public static UpdateSubscriptionRequestJsonSer getInstance() {
        if (instance == null) {
            instance = new UpdateSubscriptionRequestJsonSer();
        }
        return instance;
    }
}
