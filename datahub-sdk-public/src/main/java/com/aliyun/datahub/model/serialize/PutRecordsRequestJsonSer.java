package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.LimitExceededException;
import com.aliyun.datahub.model.PutRecordsRequest;
import com.aliyun.datahub.model.RecordEntry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.List;

public class PutRecordsRequestJsonSer implements Serializer<DefaultRequest, PutRecordsRequest> {
    @Override
    public DefaultRequest serialize(PutRecordsRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName() + "/topics/" + request.getTopicName() + "/shards");
        req.setHttpMethod(HttpMethod.POST);

        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("Action", "pub");
        ArrayNode records = node.putArray("Records");
        List<RecordEntry> list = request.getRecords();
        for (RecordEntry record : list) {
            records.add(record.toJsonNode());
        }

        String body;
        try {
            body = mapper.writeValueAsString(node);
        } catch (IOException e) {
            throw new DatahubClientException("serialize error", e);
        }
        if (body.length() > request.getRequestLimitSize()) {
            throw new LimitExceededException("request body exceed size of:" + request.getRequestLimitSize());
        }
        req.setBody(body);
        return req;
    }

    private PutRecordsRequestJsonSer() {

    }

    private static PutRecordsRequestJsonSer instance;

    public static PutRecordsRequestJsonSer getInstance() {
        if (instance == null)
            instance = new PutRecordsRequestJsonSer();
        return instance;
    }
}
