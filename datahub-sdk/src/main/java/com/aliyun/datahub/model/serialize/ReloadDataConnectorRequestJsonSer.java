package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.DatahubConstants;
import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.ReloadDataConnectorRequest;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;

public class ReloadDataConnectorRequestJsonSer implements Serializer<DefaultRequest, ReloadDataConnectorRequest> {
    @Override
    public DefaultRequest serialize(ReloadDataConnectorRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName()
                + "/topics/" + request.getTopicName()
                + "/connectors/" + request.getConnectorType().toString().toLowerCase()
        );
        req.setHttpMethod(HttpMethod.POST);

        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ObjectNode node = mapper.createObjectNode();

        node.put("Action", "reload");
        if (request.getShardId() != null) {
            node.put(DatahubConstants.ShardId, request.getShardId());
        }

        try {
            req.setBody(mapper.writeValueAsString(node));
        } catch (IOException e) {
            throw new DatahubClientException("serialize error", e);
        }
        return req;
    }

    private ReloadDataConnectorRequestJsonSer() {

    }

    private static ReloadDataConnectorRequestJsonSer instance;

    public static ReloadDataConnectorRequestJsonSer getInstance() {
        if (instance == null)
            instance = new ReloadDataConnectorRequestJsonSer();
        return instance;
    }
}
