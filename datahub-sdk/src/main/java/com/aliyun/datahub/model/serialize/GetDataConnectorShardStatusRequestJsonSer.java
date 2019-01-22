package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.DatahubConstants;
import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.GetDataConnectorShardStatusRequest;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;

public class GetDataConnectorShardStatusRequestJsonSer implements Serializer<DefaultRequest, GetDataConnectorShardStatusRequest> {
    @Override
    public DefaultRequest serialize(GetDataConnectorShardStatusRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName()
                + "/topics/" + request.getTopicName()
                + "/connectors/" + request.getConnectorType().toString().toLowerCase()
        );
        req.setHttpMethod(HttpMethod.POST);

        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ObjectNode node = mapper.createObjectNode();

        node.put("Action", "status");
        node.put(DatahubConstants.ShardId, request.getShardId());
        try {
            req.setBody(mapper.writeValueAsString(node));
        } catch (IOException e) {
            throw new DatahubClientException("serialize error", e);
        }
        return req;
    }

    private GetDataConnectorShardStatusRequestJsonSer() {

    }

    private static GetDataConnectorShardStatusRequestJsonSer instance;

    public static GetDataConnectorShardStatusRequestJsonSer getInstance() {
        if (instance == null)
            instance = new GetDataConnectorShardStatusRequestJsonSer();
        return instance;
    }
}
