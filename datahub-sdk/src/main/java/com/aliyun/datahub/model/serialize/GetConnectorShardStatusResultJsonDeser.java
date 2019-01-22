package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.GetConnectorShardStatusRequest;
import com.aliyun.datahub.model.GetConnectorShardStatusResult;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;

@Deprecated
public class GetConnectorShardStatusResultJsonDeser implements Deserializer<GetConnectorShardStatusResult,GetConnectorShardStatusRequest,Response> {
    @Override
    public GetConnectorShardStatusResult deserialize(GetConnectorShardStatusRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }
        GetConnectorShardStatusResult rs = new GetConnectorShardStatusResult();

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBody());
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        JsonNode tree = null;
        try {
            tree = mapper.readTree(is);
        } catch (IOException e) {
            throw new DatahubServiceException(
                    "JsonParseError", "Parse body failed:" + response.getBody(), response);
        }

        JsonNode node = tree.get("State");
        if (node != null && !node.isNull()) {
            rs.setState(node.asText());
        }
        node = tree.get("StartSequence");
        if (node != null && !node.isNull()) {
            rs.setStartSequence(node.asLong());
        }
        node = tree.get("EndSequence");
        if (node != null && !node.isNull()) {
            rs.setEndSequence(node.asLong());
        }
        node = tree.get("CurrentSequence");
        if (node != null && !node.isNull()) {
            rs.setCurSequence(node.asLong());
        }
        node = tree.get("UpdateTime");
        if (node != null && !node.isNull()) {
            rs.setUpdateTime(node.asLong());
        }
        node = tree.get("LastErrorMessage");
        if (node != null && !node.isNull()) {
            rs.setLastErrorMessage(node.asText());
        }

        return rs;
    }

    private GetConnectorShardStatusResultJsonDeser() {

    }

    private static GetConnectorShardStatusResultJsonDeser instance;

    public static GetConnectorShardStatusResultJsonDeser getInstance() {
        if (instance == null)
            instance = new GetConnectorShardStatusResultJsonDeser();
        return instance;
    }
}
