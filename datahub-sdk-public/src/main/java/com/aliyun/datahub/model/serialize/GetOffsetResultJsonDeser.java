package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.GetOffsetRequest;
import com.aliyun.datahub.model.GetOffsetResult;
import com.aliyun.datahub.model.OffsetContext;
import com.aliyun.datahub.rest.DatahubHttpHeaders;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class GetOffsetResultJsonDeser implements Deserializer<GetOffsetResult, GetOffsetRequest, Response> {
    @Override
    public GetOffsetResult deserialize(GetOffsetRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        GetOffsetResult rs = new GetOffsetResult();
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

        Map<String, OffsetContext.Offset> offsets = new HashMap<String, OffsetContext.Offset>();
        Map<String, Long> versions = new HashMap<String, Long>();
        Iterator<Map.Entry<String, JsonNode>> iterator = offsetsNode.fields();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> offsetNode = iterator.next();
            offsets.put(offsetNode.getKey(), new OffsetContext.Offset(offsetNode.getValue().get("Sequence").asLong(), 
            		offsetNode.getValue().get("Timestamp").asLong()));
            versions.put(offsetNode.getKey(), offsetNode.getValue().get("Version").asLong());
        }

        rs.setOffsets(offsets);
        rs.setVersions(versions);

        return rs;
    }

    private GetOffsetResultJsonDeser() {}

    private static GetOffsetResultJsonDeser instance;

    public static GetOffsetResultJsonDeser getInstance() {
        if (instance == null) {
            instance = new GetOffsetResultJsonDeser();
        }
        return instance;
    }
}
