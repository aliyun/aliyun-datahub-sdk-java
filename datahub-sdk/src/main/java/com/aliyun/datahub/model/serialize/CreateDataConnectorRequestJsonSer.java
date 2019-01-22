package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.CreateDataConnectorRequest;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.util.List;

public class CreateDataConnectorRequestJsonSer implements Serializer<DefaultRequest, CreateDataConnectorRequest> {
    @Override
    public DefaultRequest serialize(CreateDataConnectorRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName()
                + "/topics/" + request.getTopicName()
                + "/connectors/" + request.getType().toString().toLowerCase()
        );
        req.setHttpMethod(HttpMethod.POST);

        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ObjectNode node = mapper.createObjectNode();

        ArrayNode columnFields = mapper.createArrayNode();
        List<String> fields = request.getColumnFields();
        if (fields != null) {
            for (String i : request.getColumnFields()) {
                columnFields.add(i);
            }
        }
        node.put("ColumnFields", columnFields);

        node.put("Action", "create");
        ObjectNode configNode = request.getConfig().toJsonNode();
        node.put("Config", configNode);
        try {
            req.setBody(mapper.writeValueAsString(node));
        } catch (IOException e) {
            throw new DatahubClientException("serialize error", e);
        }
        return req;
    }

    private CreateDataConnectorRequestJsonSer() {

    }

    private static CreateDataConnectorRequestJsonSer instance;

    public static CreateDataConnectorRequestJsonSer getInstance() {
        if (instance == null)
            instance = new CreateDataConnectorRequestJsonSer();
        return instance;
    }
}
