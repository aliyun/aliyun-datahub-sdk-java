package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.ListDataConnectorRequest;
import com.aliyun.datahub.model.ListDataConnectorResult;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;

public class ListDataConnectorResultJsonDeser implements Deserializer<ListDataConnectorResult,ListDataConnectorRequest,Response> {
    @Override
    public ListDataConnectorResult deserialize(ListDataConnectorRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        ListDataConnectorResult rs = new ListDataConnectorResult();
        ByteArrayInputStream is = new ByteArrayInputStream(response.getBody());
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        JsonNode tree = null;
        try {
            tree = mapper.readTree(is);
        } catch (IOException e) {
            throw new DatahubServiceException(
                    "JsonParseError", "Parse body failed:" + response.getBody(), response);
        }
        JsonNode node = tree.get("Connectors");
        if (node != null && !node.isNull()) {
            if (node.isArray()) {
                Iterator<JsonNode> it = node.getElements();
                while (it.hasNext()) {
                    JsonNode connector = it.next();
                    rs.addDataConnector(connector.asText());
                }
            }
        }
        return rs;
    }

    private ListDataConnectorResultJsonDeser() {

    }

    private static ListDataConnectorResultJsonDeser instance;

    public static ListDataConnectorResultJsonDeser getInstance() {
        if (instance == null)
            instance = new ListDataConnectorResultJsonDeser();
        return instance;
    }
}
