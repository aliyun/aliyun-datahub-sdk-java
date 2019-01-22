package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.CreateTopicRequest;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;

public class CreateTopicRequestJsonSer implements Serializer<DefaultRequest, CreateTopicRequest> {

    @Override
    public DefaultRequest serialize(CreateTopicRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();

        req.setResource("/projects/" + request.getProjectName() + "/topics/" + request.getTopicName());
        req.setHttpMethod(HttpMethod.POST);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode body = mapper.createObjectNode();
        body.put("ShardCount", request.getShardCount());
        body.put("Lifecycle", request.getLifeCycle());
        body.put("RecordType", request.getRecordType().toString());

        if (request.getRecordSchema() != null) {
            body.put("RecordSchema", request.getRecordSchema().toJsonString());
        }
        body.put("Comment", request.getComment());
        try {
            req.setBody(mapper.writeValueAsString(body));
        } catch (IOException e) {
            throw new DatahubClientException("serialize error", e);
        }
        return req;
    }

    private CreateTopicRequestJsonSer() {

    }

    private static CreateTopicRequestJsonSer instance;

    public static CreateTopicRequestJsonSer getInstance() {
        if (instance == null)
            instance = new CreateTopicRequestJsonSer();
        return instance;
    }
}
