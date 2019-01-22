package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.DatahubConstants;
import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.AppendConnectorFieldRequest;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;

@Deprecated
public class AppendConnectorFieldRequestJsonSer implements Serializer<DefaultRequest, AppendConnectorFieldRequest> {
    @Override
    public DefaultRequest serialize(AppendConnectorFieldRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName()
                + "/topics/" + request.getTopicName()
                + "/connectors/" + request.getConnectorType().toString().toLowerCase()
        );
        req.setHttpMethod(HttpMethod.POST);

        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ObjectNode node = mapper.createObjectNode();

        node.put("Action", "appendfield");
        node.put(DatahubConstants.FieldName, request.getFieldName());

        try {
            req.setBody(mapper.writeValueAsString(node));
        } catch (IOException e) {
            throw new DatahubClientException("serialize error", e);
        }
        return req;
    }

    private AppendConnectorFieldRequestJsonSer() {

    }

    private static AppendConnectorFieldRequestJsonSer instance;

    public static AppendConnectorFieldRequestJsonSer getInstance() {
        if (instance == null)
            instance = new AppendConnectorFieldRequestJsonSer();
        return instance;
    }
}
