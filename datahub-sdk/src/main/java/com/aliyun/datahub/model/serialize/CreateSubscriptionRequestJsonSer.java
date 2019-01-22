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
import com.aliyun.datahub.model.CreateSubscriptionRequest;

public class CreateSubscriptionRequestJsonSer implements Serializer<DefaultRequest, CreateSubscriptionRequest> {
    @Override
    public DefaultRequest serialize(CreateSubscriptionRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setHttpMethod(HttpMethod.POST);
        req.setResource("/projects/" + request.getProjectName() + "/topics/" + request.getTopicName() + "/subscriptions");
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        try {
            ObjectNode node = mapper.createObjectNode();
            node.put("Action", "create");
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

    private CreateSubscriptionRequestJsonSer() {}

    private static CreateSubscriptionRequestJsonSer instance;

    public static CreateSubscriptionRequestJsonSer getInstance() {
        if (instance == null) {
            instance = new CreateSubscriptionRequestJsonSer();
        }
        return instance;
    }
}
