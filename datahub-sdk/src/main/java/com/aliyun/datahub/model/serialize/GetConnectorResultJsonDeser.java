package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.GetConnectorRequest;
import com.aliyun.datahub.model.GetConnectorResult;
import com.aliyun.datahub.model.OdpsDesc;
import com.aliyun.datahub.model.ShardContext;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
@Deprecated
public class GetConnectorResultJsonDeser implements Deserializer<GetConnectorResult,GetConnectorRequest,Response> {
    @Override
    public GetConnectorResult deserialize(GetConnectorRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        GetConnectorResult rs = new GetConnectorResult();
        ByteArrayInputStream is = new ByteArrayInputStream(response.getBody());
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        JsonNode tree = null;
        try {
            tree = mapper.readTree(is);
        } catch (IOException e) {
            throw new DatahubServiceException(
                    "JsonParseError", "Parse body failed:" + response.getBody(), response);
        }

        List<String> columnFields = new ArrayList<String>();
        JsonNode node = tree.get("ColumnFields");
        if (node != null && !node.isNull()) {
            if (node.isArray()) {
                Iterator<JsonNode> it = node.getElements();
                while (it.hasNext()) {
                    JsonNode field = it.next();
                    columnFields.add(field.asText());
                }
            }
        }
        rs.setColumnFields(columnFields);

        node = tree.get("Type");
        if (node != null && !node.isNull()) {
            rs.setType(node.asText());
        }

        node = tree.get("State");
        if (node != null && !node.isNull()) {
            rs.setState(node.asText());
        }
        node = tree.get("Creator");
        if (node != null && !node.isNull()) {
            rs.setCreator(node.asText());
        }
        node = tree.get("Owner");
        if (node != null && !node.isNull()) {
            rs.setOwner(node.asText());
        }
        OdpsDesc odpsDesc = new OdpsDesc();
        node = tree.get("Config");
        if (node != null && !node.isNull()) {
            JsonNode odps = node.get("Project");
            if (odps != null && !odps.isNull()) {
                odpsDesc.setProject(odps.asText());
            }
            odps = node.get("Table");
            if (odps != null && !odps.isNull()) {
                odpsDesc.setTable(odps.asText());
            }
            odps = node.get("OdpsEndpoint");
            if (odps != null && !odps.isNull()) {
                odpsDesc.setOdpsEndpoint(odps.asText());
            }
            odps = node.get("TunnelEndpoint");
            if (odps != null && !odps.isNull()) {
                odpsDesc.setTunnelEndpoint(odps.asText());
            }
        }
        rs.setOdpsDesc(odpsDesc);
        List<ShardContext> shardContexts = new ArrayList<ShardContext>();
        node = tree.get("ShardContexts");
        if (node != null && !node.isNull() && node.isArray()) {
            Iterator<JsonNode> it = node.getElements();
            while (it.hasNext()) {
                ShardContext shardContext = new ShardContext();
                JsonNode jsonNode = it.next();
                JsonNode context = jsonNode.get("ShardId");
                if (context != null && !context.isNull()) {
                    shardContext.setShardId(context.asText());
                }
                context = jsonNode.get("StartSequence");
                if (context != null && !context.isNull()) {
                    shardContext.setStartSequence(context.asLong());
                }
                context = jsonNode.get("EndSequence");
                if (context != null && !context.isNull()) {
                    shardContext.setEndSequence(context.asLong());
                }
                context = jsonNode.get("CurrentSequence");
                if (context != null && !context.isNull()) {
                    shardContext.setCurSequence(context.asLong());
                }
                shardContexts.add(shardContext);
            }
            rs.setShardContext(shardContexts);

        }
        return rs;
    }

    private GetConnectorResultJsonDeser() {

    }

    private static GetConnectorResultJsonDeser instance;

    public static GetConnectorResultJsonDeser getInstance() {
        if (instance == null)
            instance = new GetConnectorResultJsonDeser();
        return instance;
    }
}
