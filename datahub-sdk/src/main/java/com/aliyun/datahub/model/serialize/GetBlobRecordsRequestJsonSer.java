package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.InvalidParameterException;
import com.aliyun.datahub.model.GetBlobRecordsRequest;
import com.aliyun.datahub.model.GetRecordsRequest;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;

public class GetBlobRecordsRequestJsonSer implements Serializer<DefaultRequest, GetBlobRecordsRequest> {

    @Override
    public DefaultRequest serialize(GetBlobRecordsRequest request) throws DatahubClientException {
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

    private GetBlobRecordsRequestJsonSer() {

    }

    private static GetBlobRecordsRequestJsonSer instance;

    public static GetBlobRecordsRequestJsonSer getInstance() {
        if (instance == null)
            instance = new GetBlobRecordsRequestJsonSer();
        return instance;
    }
}
