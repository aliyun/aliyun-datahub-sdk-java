package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.InitOffsetContextRequest;
import com.aliyun.datahub.model.InitOffsetContextResult;
import com.aliyun.datahub.model.OffsetContext;
import com.aliyun.datahub.rest.DatahubHttpHeaders;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class InitOffsetContextResultJsonDeser implements Deserializer<InitOffsetContextResult, InitOffsetContextRequest, Response> {
    @Override
    public InitOffsetContextResult deserialize(InitOffsetContextRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        InitOffsetContextResult rs = new InitOffsetContextResult();
        rs.setRequestId(response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));

        ObjectMapper mapper = JacksonParser.getObjectMapper();

        JsonNode tree = null;
        try {
            tree = mapper.readTree(response.getBody());
        } catch (IOException e) {
            throw new DatahubServiceException(
                    "JsonParseError", "Parse body failed:" + response.getBody(), response);
        }

        JsonNode offsetsNode = tree.get("Offsets");
        if (!offsetsNode.isObject()) {
            throw new DatahubServiceException("JsonParseError", "invalid offsets node value", response);
        }

        Map<String, OffsetContext> offsets = new HashMap<String, OffsetContext>();
        Iterator<Map.Entry<String, JsonNode>> iterator = offsetsNode.fields();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> offsetNode = iterator.next();
            OffsetContext offset = new OffsetContext(request.getProjectName(), request.getTopicName(), request.getSubId(), offsetNode.getKey(),
            		new OffsetContext.Offset(offsetNode.getValue().get("Sequence").asLong(),
            		offsetNode.getValue().get("Timestamp").asLong()),
            		offsetNode.getValue().get("Version").asLong(),
            		offsetNode.getValue().get("SessionId").asText());
            offsets.put(offsetNode.getKey(), offset);
        }

        rs.setOffsets(offsets);

        return rs;
    }

    private InitOffsetContextResultJsonDeser() {}

    private static InitOffsetContextResultJsonDeser instance;

    public static InitOffsetContextResultJsonDeser getInstance() {
        if (instance == null) {
            instance = new InitOffsetContextResultJsonDeser();
        }
        return instance;
    }
}
