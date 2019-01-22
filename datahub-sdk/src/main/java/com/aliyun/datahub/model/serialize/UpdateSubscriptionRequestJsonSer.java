package com.aliyun.datahub.model.serialize;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.UpdateSubscriptionRequest;

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
