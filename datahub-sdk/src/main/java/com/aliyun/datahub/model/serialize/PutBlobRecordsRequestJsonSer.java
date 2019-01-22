package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.BlobRecordEntry;
import com.aliyun.datahub.model.PutBlobRecordsRequest;
import com.aliyun.datahub.model.PutRecordsRequest;
import com.aliyun.datahub.model.RecordEntry;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.util.List;

public class PutBlobRecordsRequestJsonSer implements Serializer<DefaultRequest, PutBlobRecordsRequest> {
    @Override
    public DefaultRequest serialize(PutBlobRecordsRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName() + "/topics/" + request.getTopicName() + "/shards");
        req.setHttpMethod(HttpMethod.POST);

        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("Action", "pub");
        ArrayNode records = node.putArray("Records");
        List<BlobRecordEntry> list = request.getRecords();
        for (BlobRecordEntry record : list) {
            records.add(record.toJsonNode());
        }
        try {
            req.setBody(mapper.writeValueAsString(node));
        } catch (IOException e) {
            throw new DatahubClientException("serialize error", e);
        }

        return req;
    }

    private PutBlobRecordsRequestJsonSer() {

    }

    private static PutBlobRecordsRequestJsonSer instance;

    public static PutBlobRecordsRequestJsonSer getInstance() {
        if (instance == null)
            instance = new PutBlobRecordsRequestJsonSer();
        return instance;
    }
}
