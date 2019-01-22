package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.DatahubConstants;
import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.ReloadConnectorRequest;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
@Deprecated
public class ReloadConnectorRequestJsonSer implements Serializer<DefaultRequest, ReloadConnectorRequest> {
    @Override
    public DefaultRequest serialize(ReloadConnectorRequest request) throws DatahubClientException {
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

    private ReloadConnectorRequestJsonSer() {

    }

    private static ReloadConnectorRequestJsonSer instance;

    public static ReloadConnectorRequestJsonSer getInstance() {
        if (instance == null)
            instance = new ReloadConnectorRequestJsonSer();
        return instance;
    }
}
