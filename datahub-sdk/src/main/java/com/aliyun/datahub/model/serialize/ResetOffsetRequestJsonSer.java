package com.aliyun.datahub.model.serialize;

import java.io.IOException;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.OffsetContext;
import com.aliyun.datahub.model.ResetOffsetRequest;

public class ResetOffsetRequestJsonSer implements Serializer<DefaultRequest, ResetOffsetRequest> {
    @Override
    public DefaultRequest serialize(ResetOffsetRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName() + "/topics/" + request.getTopicName() + "/subscriptions/" + request.getSubId() + "/offsets");
        req.setHttpMethod(HttpMethod.PUT);

        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("Action", "reset");
        ObjectNode offsets = node.putObject("Offsets");
        for (Map.Entry<String, OffsetContext.Offset> offset: request.getOffsets().entrySet()) {
            ObjectNode offsetNode = offsets.putObject(offset.getKey());
            offsetNode.put("Sequence", offset.getValue().getSequence());
            offsetNode.put("Timestamp", offset.getValue().getTimestamp());
        }

        try {
            req.setBody(mapper.writeValueAsString(node));
        } catch (IOException e) {
            throw new DatahubClientException("serialize error", e);
        }

        return req;
    }

    private ResetOffsetRequestJsonSer() {}

    private static ResetOffsetRequestJsonSer instance;

    public static ResetOffsetRequestJsonSer getInstance() {
        if (instance == null) {
            instance = new ResetOffsetRequestJsonSer();
        }
        return instance;
    }
}
