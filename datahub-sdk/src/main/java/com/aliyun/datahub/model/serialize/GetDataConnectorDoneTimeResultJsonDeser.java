package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.*;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class GetDataConnectorDoneTimeResultJsonDeser implements Deserializer<GetDataConnectorDoneTimeResult,GetDataConnectorDoneTimeRequest,Response> {
    @Override
    public GetDataConnectorDoneTimeResult deserialize(GetDataConnectorDoneTimeRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        GetDataConnectorDoneTimeResult rs = new GetDataConnectorDoneTimeResult();
        ByteArrayInputStream is = new ByteArrayInputStream(response.getBody());
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        JsonNode tree = null;
        try {
            tree = mapper.readTree(is);
        } catch (IOException e) {
            throw new DatahubServiceException(
                    "JsonParseError", "Parse body failed:" + response.getBody(), response);
        }

        JsonNode node = tree.get("DoneTime");
        if (node != null && !node.isNull()) {
            rs.setDoneTime(node.asLong());
        }
        return rs;
    }

    private GetDataConnectorDoneTimeResultJsonDeser() {

    }

    private static GetDataConnectorDoneTimeResultJsonDeser instance;

    public static GetDataConnectorDoneTimeResultJsonDeser getInstance() {
        if (instance == null)
            instance = new GetDataConnectorDoneTimeResultJsonDeser();
        return instance;
    }
}
