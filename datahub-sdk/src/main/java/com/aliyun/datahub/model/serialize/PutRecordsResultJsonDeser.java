package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.DatahubConstants;
import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.exception.InvalidParameterException;
import com.aliyun.datahub.model.*;
import com.aliyun.datahub.rest.DatahubHttpHeaders;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;

public class PutRecordsResultJsonDeser implements Deserializer<PutRecordsResult, PutRecordsRequest, Response> {

    @Override
    public PutRecordsResult deserialize(PutRecordsRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        PutRecordsResult rs = new PutRecordsResult();
        rs.setRequestId(response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBody());
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        JsonNode tree = null;
        try {
            tree = mapper.readTree(is);
        } catch (IOException e) {
            throw new DatahubServiceException(
                    "JsonParseError", "Parse body failed:" + response.getBody(), response);
        }
        JsonNode node = tree.get("FailedRecordCount");
        if (node != null && !node.isNull()) {
            if (node.isInt()) {
                rs.setFailedRecordCount(node.asInt());
            }
        }

        node = tree.get("FailedRecords");
        if (node != null && !node.isNull()) {
            if (!node.isArray()) {
                throw new DatahubServiceException(
                        "InvalidResultFormat", "Invalid result format:" + tree.asText(), response);
            }

            Iterator<JsonNode> failedNodes = node.getElements();
            while (failedNodes.hasNext()) {
                JsonNode failedNode = failedNodes.next();
                int index = failedNode.get("Index").asInt();
                String errCode = failedNode.get("ErrorCode").asText();
                String errMsg = failedNode.get("ErrorMessage").asText();
                ErrorEntry error = new ErrorEntry(errCode, errMsg);
                rs.addFailedIndex(index);
                rs.addFailedError(error);
            }
        }
        return rs;
    }

    private static PutRecordsResultJsonDeser instance;

    private PutRecordsResultJsonDeser() {

    }

    public static PutRecordsResultJsonDeser getInstance() {
        if (instance == null)
            instance = new PutRecordsResultJsonDeser();
        return instance;
    }
}
