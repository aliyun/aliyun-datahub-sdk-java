package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.DatahubConstants;
import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.AppendDataConnectorFieldRequest;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;

public class AppendDataConnectorFieldRequestJsonSer implements Serializer<DefaultRequest, AppendDataConnectorFieldRequest> {
    @Override
    public DefaultRequest serialize(AppendDataConnectorFieldRequest request) throws DatahubClientException {
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

    private AppendDataConnectorFieldRequestJsonSer() {

    }

    private static AppendDataConnectorFieldRequestJsonSer instance;

    public static AppendDataConnectorFieldRequestJsonSer getInstance() {
        if (instance == null)
            instance = new AppendDataConnectorFieldRequestJsonSer();
        return instance;
    }
}
