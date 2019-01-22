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

public class GetDataConnectorResultJsonDeser implements Deserializer<GetDataConnectorResult,GetDataConnectorRequest,Response> {
    @Override
    public GetDataConnectorResult deserialize(GetDataConnectorRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        GetDataConnectorResult rs = new GetDataConnectorResult();
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
        ConnectorConfig config;
        node = tree.get("Config");
        if (rs.getType() == ConnectorType.SINK_ODPS) {
            config = new OdpsDesc();
            config.ParseFromJsonNode(node);
        } else if (rs.getType() == ConnectorType.SINK_ES) {
            config = new ElasticSearchDesc();
            config.ParseFromJsonNode(node);
        } else if (rs.getType() == ConnectorType.SINK_DATAHUB) {
            config = new DatahubDesc();
            config.ParseFromJsonNode(node);
        } else if (rs.getType() == ConnectorType.SINK_OSS) {
            config = new OssDesc();
            config.ParseFromJsonNode(node);
        } else if (rs.getType() == ConnectorType.SINK_ADS || rs.getType() == ConnectorType.SINK_MYSQL) {
            config = new DatabaseDesc();
            config.ParseFromJsonNode(node);
        } else if (rs.getType() == ConnectorType.SINK_FC) {
            config = new FcDesc();
            config.ParseFromJsonNode(node);
        } else if (rs.getType() == ConnectorType.SINK_OTS) {
            config = new OtsDesc();
            config.ParseFromJsonNode(node);
        } else {
            throw new DatahubClientException("Invalid response, missing 'config'");
        }

        rs.setConnectorConfig(config);
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

    private GetDataConnectorResultJsonDeser() {

    }

    private static GetDataConnectorResultJsonDeser instance;

    public static GetDataConnectorResultJsonDeser getInstance() {
        if (instance == null)
            instance = new GetDataConnectorResultJsonDeser();
        return instance;
    }
}
