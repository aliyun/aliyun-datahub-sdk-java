package com.aliyun.datahub.model.serialize;

import java.util.Map;
import com.aliyun.datahub.model.OffsetContext;
import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.CommitOffsetRequest;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;

public class CommitOffsetRequestJsonSer implements Serializer<DefaultRequest, CommitOffsetRequest> {
    @Override
    public DefaultRequest serialize(CommitOffsetRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProject() + "/topics/" + request.getTopic() + "/subscriptions/" + request.getSubId() + "/offsets");
        req.setHttpMethod(HttpMethod.PUT);

        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("Action", "commit");
        ObjectNode offsets = node.putObject("Offsets");
        
        for (Map.Entry<String, OffsetContext> offset: request.getOffsetCtxMap().entrySet()) {
        	ObjectNode offsetNode = offsets.putObject(offset.getKey());
        	offsetNode.put("Sequence", offset.getValue().getOffset().getSequence());
        	offsetNode.put("Timestamp", offset.getValue().getOffset().getTimestamp());
        	offsetNode.put("Version", offset.getValue().getVersion());
        	offsetNode.put("SessionId", Long.parseLong(offset.getValue().getSessionId()));
        }

        try {
            req.setBody(mapper.writeValueAsString(node));
        } catch (IOException e) {
            throw new DatahubClientException("serialize error", e);
        }

        return req;
    }

    private CommitOffsetRequestJsonSer() {}

    private static CommitOffsetRequestJsonSer instance;

    public static CommitOffsetRequestJsonSer getInstance() {
        if (instance == null) {
            instance = new CommitOffsetRequestJsonSer();
        }
        return instance;
    }
}
