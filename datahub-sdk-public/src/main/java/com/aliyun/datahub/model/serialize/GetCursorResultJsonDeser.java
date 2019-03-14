package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.GetCursorRequest;
import com.aliyun.datahub.model.GetCursorResult;
import com.aliyun.datahub.rest.DatahubHttpHeaders;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class GetCursorResultJsonDeser implements Deserializer<GetCursorResult, GetCursorRequest, Response> {

    @Override
    public GetCursorResult deserialize(GetCursorRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        GetCursorResult res = new GetCursorResult();
        res.setRequestId(response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));

        ObjectMapper mapper = JacksonParser.getObjectMapper();
        JsonNode tree = null;

        try {
            tree = mapper.readTree(response.getBody());
        } catch (IOException e) {
            throw new DatahubServiceException(
                    "JsonParseError", "Parse body failed:" + response.getBody(), response);
        }

        res.setCursor(tree.get("Cursor").asText());
        res.setRecordTime(tree.get("RecordTime").asLong());
        res.setSequence(tree.get("Sequence").asLong());
        return res;
    }

    private GetCursorResultJsonDeser() {

    }

    private static GetCursorResultJsonDeser instance;

    public static GetCursorResultJsonDeser getInstance() {
        if (instance == null)
            instance = new GetCursorResultJsonDeser();
        return instance;
    }
}
