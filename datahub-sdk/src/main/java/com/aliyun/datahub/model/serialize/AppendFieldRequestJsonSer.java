package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.AppendFieldRequest;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;

public class AppendFieldRequestJsonSer implements Serializer<DefaultRequest,AppendFieldRequest> {
    @Override
    public DefaultRequest serialize(AppendFieldRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setHttpMethod(HttpMethod.POST);
        req.setResource("/projects/" + request.getProjectName() + "/topics/" + request.getTopicName());
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode body = mapper.createObjectNode();
        body.put("Action", "appendfield");
        body.put("FieldName", request.getField().getName());
        body.put("FieldType", request.getField().getType().toString());

        try {
            req.setBody(mapper.writeValueAsString(body));
        } catch (IOException e) {
            throw new DatahubClientException("serialize error", e);
        }
        return req;
    }
    private AppendFieldRequestJsonSer() {

    }

    private static AppendFieldRequestJsonSer instance;

    public static AppendFieldRequestJsonSer getInstance() {
        if (instance == null)
            instance = new AppendFieldRequestJsonSer();
        return instance;
    }
}
