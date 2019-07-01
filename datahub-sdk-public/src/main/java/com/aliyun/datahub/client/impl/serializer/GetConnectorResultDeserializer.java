package com.aliyun.datahub.client.impl.serializer;

import com.aliyun.datahub.client.model.*;
import com.aliyun.datahub.client.util.JsonUtils;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetConnectorResultDeserializer extends JsonDeserializer<GetConnectorResult> {
    @Override
    public GetConnectorResult deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
        GetConnectorResult result = new GetConnectorResult();
        JsonNode tree = jsonParser.getCodec().readTree(jsonParser);
        JsonNode node = tree.get("ColumnFields");
        if (node != null && node.isArray()) {
            List<String> columnFields = new ArrayList<>();
            for (JsonNode fNode : node) {
                columnFields.add(fNode.asText());
            }
            result.setColumnFields(columnFields);
        }
        node = tree.get("Type");
        if (node != null && !node.isNull()) {
            result.setType(ConnectorType.valueOf(node.asText().toUpperCase()));
        }
        node = tree.get("State");
        if (node != null && !node.isNull()) {
            result.setState(ConnectorState.fromString(node.asText().toUpperCase()));
        }
        node = tree.get("ShardContexts");
        if (node != null && node.isArray()) {
            List<ShardContext> shardContexts = new ArrayList<>();
            for (JsonNode fNode : node) {
                ShardContext context = JsonUtils.fromJsonNode(fNode, ShardContext.class);
                if (context == null) {
                    throw new IOException("ShardContext format is invalid");
                }
                shardContexts.add(context);
            }
            result.setShardContexts(shardContexts);
        }
        node = tree.get("ExtraInfo");
        if (node != null && node.isObject()) {
            node = node.get("SubscriptionId");
            if (node != null && node.isTextual()) {
                result.setSubId(node.asText());
            }
        }

        node = tree.get("Config");
        if (node != null && node.isObject()) {
            SinkConfig config = null;
            switch (result.getType()) {
                case SINK_ODPS:
                    config = deserializeOdpsConfig(node);
                    break;
                case SINK_ADS:
                case SINK_MYSQL:
                    config = deserializeDatabaseConfig(node);
                    break;
                case SINK_DATAHUB:
                    config = deserializeDatahubConfig(node);
                    break;
                case SINK_ES:
                    config = deserializeEsConfig(node);
                    break;
                case SINK_FC:
                    config = deserializeFcConfig(node);
                    break;
                case SINK_OTS:
                    config = deserializeOtsConfig(node);
                    break;
                case SINK_OSS:
                    config = deserializeOssConfig(node);
                    break;
            }
            result.setConfig(config);
        }

        return result;
    }

    private SinkConfig deserializeOdpsConfig(JsonNode tree) {
        SinkOdpsConfig config = new SinkOdpsConfig();
        JsonNode node = tree.get("Project");
        if (node != null && node.isTextual()) {
            config.setProject(node.asText());
        }
        node = tree.get("Table");
        if (node != null && node.isTextual()) {
            config.setTable(node.asText());
        }
        node = tree.get("OdpsEndpoint");
        if (node != null && node.isTextual()) {
            config.setEndpoint(node.asText());
        }
        node = tree.get("TunnelEndpoint");
        if (node != null && node.isTextual()) {
            config.setTunnelEndpoint(node.asText());
        }
        node = tree.get("PartitionMode");
        if (node != null && node.isTextual()) {
            config.setPartitionMode(SinkOdpsConfig.PartitionMode.valueOf(node.asText().toUpperCase()));
        }
        node = tree.get("TimeRange");
        if (node != null && node.isTextual()) {
            config.setTimeRange(Integer.valueOf(node.asText()));
        }
        node = tree.get("PartitionConfig");
        if (node != null && node.isTextual()) {
            node = JsonUtils.toJsonNode(node.asText());
            if (node != null && node.isArray()) {
                SinkOdpsConfig.PartitionConfig partitionConfig = new SinkOdpsConfig.PartitionConfig();
                for (JsonNode fNode : node) {
                    JsonNode temp = fNode.get("key");
                    String key = temp.asText();
                    temp = fNode.get("value");
                    String value = temp.asText();
                    partitionConfig.addConfig(key, value);
                }
                config.setPartitionConfig(partitionConfig);
            }
        }
        return config;
    }

    private SinkConfig deserializeDatabaseConfig(JsonNode tree) {
        SinkMysqlConfig config = new SinkMysqlConfig();
        JsonNode node = tree.get("Host");
        if (node != null && node.isTextual()) {
            config.setHost(node.asText());
        }
        node = tree.get("Port");
        if (node != null && node.isTextual()) {
            config.setPort(Integer.valueOf(node.asText()));
        }
        node = tree.get("Database");
        if (node != null && node.isTextual()) {
            config.setDatabase(node.asText());
        }
        node = tree.get("Table");
        if (node != null && node.isTextual()) {
            config.setTable(node.asText());
        }

        node = tree.get("Ignore");
        if (node != null && node.isTextual()) {
            config.setInsertMode(Boolean.valueOf(node.asText()) ?
                    SinkMysqlConfig.InsertMode.IGNORE : SinkMysqlConfig.InsertMode.OVERWRITE);
        }
        return config;
    }

    private SinkConfig deserializeDatahubConfig(JsonNode tree) {
        SinkDatahubConfig config = new SinkDatahubConfig();
        JsonNode node = tree.get("Endpoint");
        if (node != null && node.isTextual()) {
            config.setEndpoint(node.asText());
        }
        node = tree.get("Project");
        if (node != null && node.isTextual()) {
            config.setProjectName(node.asText());
        }
        node = tree.get("Topic");
        if (node != null && node.isTextual()) {
            config.setTopicName(node.asText());
        }
        return config;
    }

    private SinkConfig deserializeEsConfig(JsonNode tree) {
        SinkEsConfig config = new SinkEsConfig();
        JsonNode node = tree.get("Index");
        if (node != null && node.isTextual()) {
            config.setIndex(node.asText());
        }
        node = tree.get("Endpoint");
        if (node != null && node.isTextual()) {
            config.setEndpoint(node.asText());
        }
        node = tree.get("IDFields");
        if (node != null && node.isTextual()) {
            JsonNode fNode = JsonUtils.toJsonNode(node.asText());
            if (fNode != null && fNode.isArray()) {
                List<String> idFields = new ArrayList<>();
                for (JsonNode temp : fNode) {
                    idFields.add(temp.asText());
                }
                config.setIdFields(idFields);
            }
        }
        node = tree.get("TypeFields");
        if (node != null && node.isTextual()) {
            JsonNode fNode = JsonUtils.toJsonNode(node.asText());
            if (fNode != null && fNode.isArray()) {
                List<String> typeFields = new ArrayList<>();
                for (JsonNode temp : fNode) {
                    typeFields.add(temp.asText());
                }
                config.setTypeFields(typeFields);
            }
        }

        node = tree.get("ProxyMode");
        if (node != null && node.isTextual()) {
            config.setProxyMode(Boolean.valueOf(node.asText()));
        }
        return config;
    }

    private SinkConfig deserializeFcConfig(JsonNode tree) {
        SinkFcConfig config = new SinkFcConfig();
        JsonNode node = tree.get("Endpoint");
        if (node != null && node.isTextual()) {
            config.setEndpoint(node.asText());
        }
        node = tree.get("Service");
        if (node != null && node.isTextual()) {
            config.setService(node.asText());
        }
        node = tree.get("Function");
        if (node != null && node.isTextual()) {
            config.setFunction(node.asText());
        }
        node = tree.get("AuthMode");
        if (node != null && node.isTextual()) {
            config.setAuthMode(SinkConfig.AuthMode.valueOf(node.asText().toUpperCase()));
        }
        return config;
    }

    private SinkConfig deserializeOtsConfig(JsonNode tree) {
        SinkOtsConfig config = new SinkOtsConfig();
        JsonNode node = tree.get("Endpoint");
        if (node != null && node.isTextual()) {
            config.setEndpoint(node.asText());
        }
        node = tree.get("InstanceName");
        if (node != null && node.isTextual()) {
            config.setInstance(node.asText());
        }
        node = tree.get("TableName");
        if (node != null && node.isTextual()) {
            config.setTable(node.asText());
        }
        node = tree.get("AuthMode");
        if (node != null && node.isTextual()) {
            config.setAuthMode(SinkConfig.AuthMode.valueOf(node.asText().toUpperCase()));
        }
//        node = tree.get("AccessId");
//        if (node != null && node.isTextual()) {
//            config.setAccessId(node.asText());
//        }
//        node = tree.get("AccessKey");
//        if (node != null && node.isTextual()) {
//            config.setAccessKey(node.asText());
//        }
        return config;
    }

    private SinkConfig deserializeOssConfig(JsonNode tree) {
        SinkOssConfig config = new SinkOssConfig();
        JsonNode node = tree.get("Endpoint");
        if (node != null && node.isTextual()) {
            config.setEndpoint(node.asText());
        }
        node = tree.get("Bucket");
        if (node != null && node.isTextual()) {
            config.setBucket(node.asText());
        }
        node = tree.get("Prefix");
        if (node != null && node.isTextual()) {
            config.setPrefix(node.asText());
        }
        node = tree.get("TimeFormat");
        if (node != null && node.isTextual()) {
            config.setTimeFormat(node.asText());
        }
        node = tree.get("TimeRange");
        if (node != null && node.isTextual()) {
            config.setTimeRange(Integer.valueOf(node.asText()));
        }
        node = tree.get("AuthMode");
        if (node != null && node.isTextual()) {
            config.setAuthMode(SinkConfig.AuthMode.valueOf(node.asText().toUpperCase()));
        }
//        node = tree.get("AccessId");
//        if (node != null && node.isTextual()) {
//            config.setAccessId(node.asText());
//        }
//        node = tree.get("AccessKey");
//        if (node != null && node.isTextual()) {
//            config.setAccessKey(node.asText());
//        }
        return config;
    }
}