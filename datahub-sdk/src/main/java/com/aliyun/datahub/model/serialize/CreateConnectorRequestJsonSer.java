package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.CreateConnectorRequest;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
@Deprecated
public class CreateConnectorRequestJsonSer implements Serializer<DefaultRequest, CreateConnectorRequest> {
    @Override
    public DefaultRequest serialize(CreateConnectorRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName()
                + "/topics/" + request.getTopicName()
                + "/connectors/" + request.getType().toString().toLowerCase()
        );
        req.setHttpMethod(HttpMethod.POST);

        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ObjectNode node = mapper.createObjectNode();

        ArrayNode columnFields = mapper.createArrayNode();
        for (String i : request.getColumnFields()) {
            columnFields.add(i);
        }
        node.put("ColumnFields", columnFields);

        // TODO support different config
        node.put("Action", "create");
        ObjectNode odpsDesc = mapper.createObjectNode();
        odpsDesc.put("Project", request.getOdpsDesc().getProject());
        odpsDesc.put("Table", request.getOdpsDesc().getTable());
        odpsDesc.put("OdpsEndpoint", request.getOdpsDesc().getOdpsEndpoint());
        if (request.getOdpsDesc().getTunnelEndpoint() != null) {
            odpsDesc.put("TunnelEndpoint", request.getOdpsDesc().getTunnelEndpoint());
        }
        odpsDesc.put("AccessId", request.getOdpsDesc().getAccessId());
        odpsDesc.put("AccessKey", request.getOdpsDesc().getAccessKey());
        node.put("Config", odpsDesc);
        try {
            req.setBody(mapper.writeValueAsString(node));
        } catch (IOException e) {
            throw new DatahubClientException("serialize error", e);
        }
        return req;
    }

    private CreateConnectorRequestJsonSer() {

    }

    private static CreateConnectorRequestJsonSer instance;

    public static CreateConnectorRequestJsonSer getInstance() {
        if (instance == null)
            instance = new CreateConnectorRequestJsonSer();
        return instance;
    }
}
