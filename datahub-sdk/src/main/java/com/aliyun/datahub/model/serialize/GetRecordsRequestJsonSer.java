package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.InvalidParameterException;
import com.aliyun.datahub.model.GetRecordsRequest;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;

public class GetRecordsRequestJsonSer implements Serializer<DefaultRequest, GetRecordsRequest> {

    @Override
    public DefaultRequest serialize(GetRecordsRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName() + "/topics/"
                + request.getTopicName() + "/shards/" + request.getShardId());

        req.setHttpMethod(HttpMethod.POST);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode body = mapper.createObjectNode();
        body.put("Action", "sub");
        body.put("Cursor", request.getCursor());
        if (request.getLimit() <= 0) {
            throw new InvalidParameterException("record size must be positive");
        }
        body.put("Limit", request.getLimit());
        try {
            req.setBody(mapper.writeValueAsString(body));
        } catch (IOException e) {
            throw new DatahubClientException("serialize error", e);
        }
        return req;
    }

    private GetRecordsRequestJsonSer() {

    }

    private static GetRecordsRequestJsonSer instance;

    public static GetRecordsRequestJsonSer getInstance() {
        if (instance == null)
            instance = new GetRecordsRequestJsonSer();
        return instance;
    }
}
