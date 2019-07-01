package com.aliyun.datahub.client.impl.serializer;

import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.model.*;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.Map;

public class SinkConfigSerializer extends JsonSerializer<SinkConfig> {
    @Override
    public void serialize(SinkConfig sinkConfig, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
        if (sinkConfig instanceof SinkOdpsConfig) {
            serializeOdpsConfig((SinkOdpsConfig) sinkConfig, jsonGenerator);
        } else if (sinkConfig instanceof SinkMysqlConfig) {
            serializeDatabaseConfig((SinkMysqlConfig) sinkConfig, jsonGenerator);
        } else if (sinkConfig instanceof SinkDatahubConfig) {
            serializeDatahubConfig((SinkDatahubConfig) sinkConfig, jsonGenerator);
        } else if (sinkConfig instanceof SinkEsConfig) {
            serializeEsConfig((SinkEsConfig) sinkConfig, jsonGenerator);
        } else if (sinkConfig instanceof SinkFcConfig) {
            serializeFcConfig((SinkFcConfig) sinkConfig, jsonGenerator);
        } else if (sinkConfig instanceof SinkOtsConfig) {
            serializeOtsConfig((SinkOtsConfig) sinkConfig, jsonGenerator);
        } else if (sinkConfig instanceof SinkOssConfig) {
            serializeOssConfig((SinkOssConfig) sinkConfig, jsonGenerator);
        } else {
            throw new IOException("Unknown connector config type");
        }
    }

    private void serializeOdpsConfig(SinkOdpsConfig config, JsonGenerator jsonGenerator) throws IOException, JsonProcessingException {
        jsonGenerator.writeStartObject();

        jsonGenerator.writeStringField("Project", config.getProject());
        jsonGenerator.writeStringField("Table", config.getTable());
        jsonGenerator.writeStringField("OdpsEndpoint", config.getEndpoint());
        if (config.getTunnelEndpoint() != null && !config.getTunnelEndpoint().isEmpty()) {
            jsonGenerator.writeStringField("TunnelEndpoint", config.getTunnelEndpoint());
        }
        jsonGenerator.writeStringField("AccessId", config.getAccessId());
        jsonGenerator.writeStringField("AccessKey", config.getAccessKey());

        if (config.getPartitionMode() == null) {
            throw new InvalidParameterException("PartitionMode should not null");
        }
        jsonGenerator.writeStringField("PartitionMode", config.getPartitionMode().name());
        if (config.getPartitionMode() != SinkOdpsConfig.PartitionMode.USER_DEFINE) {
            jsonGenerator.writeNumberField("TimeRange", config.getTimeRange());
        }

        if (config.getPartitionConfig() != null) {
            jsonGenerator.writeObjectFieldStart("PartitionConfig");
            for (Map.Entry<String, String> entry : config.getPartitionConfig().getConfigMap().entrySet()) {
                jsonGenerator.writeStringField(entry.getKey(), entry.getValue());
            }
            jsonGenerator.writeEndObject();
        }

        jsonGenerator.writeEndObject();
    }

    private void serializeDatabaseConfig(SinkMysqlConfig config, JsonGenerator jsonGenerator) throws IOException, JsonProcessingException {
        jsonGenerator.writeStartObject();

        jsonGenerator.writeStringField("Host", config.getHost());
        jsonGenerator.writeStringField("Port", String.valueOf(config.getPort()));
        jsonGenerator.writeStringField("Database", config.getDatabase());
        jsonGenerator.writeStringField("Table", config.getTable());
        jsonGenerator.writeStringField("User", config.getUser());
        jsonGenerator.writeStringField("Password", config.getPassword());
        jsonGenerator.writeStringField("Ignore", String.valueOf(config.getInsertMode() == SinkMysqlConfig.InsertMode.IGNORE));

        jsonGenerator.writeEndObject();
    }

    private void serializeDatahubConfig(SinkDatahubConfig config, JsonGenerator jsonGenerator) throws IOException, JsonProcessingException {
        jsonGenerator.writeStartObject();

        jsonGenerator.writeStringField("Endpoint", config.getEndpoint());
        jsonGenerator.writeStringField("Project", config.getProjectName());
        jsonGenerator.writeStringField("Topic", config.getTopicName());
        jsonGenerator.writeStringField("AuthMode", config.getAuthMode().name().toLowerCase());
        if (config.getAuthMode() == SinkConfig.AuthMode.AK) {
            jsonGenerator.writeStringField("AccessId", config.getAccessId());
            jsonGenerator.writeStringField("AccessKey", config.getAccessKey());
        }

        jsonGenerator.writeEndObject();
    }

    private void serializeEsConfig(SinkEsConfig config, JsonGenerator jsonGenerator) throws IOException, JsonProcessingException {
        jsonGenerator.writeStartObject();

        jsonGenerator.writeStringField("Index", config.getIndex());
        jsonGenerator.writeStringField("Endpoint", config.getEndpoint());
        jsonGenerator.writeStringField("User", config.getUser());
        jsonGenerator.writeStringField("Password", config.getPassword());
        if (config.getIdFields() != null) {
            jsonGenerator.writeArrayFieldStart("IDFields");
            for (String field : config.getIdFields()) {
                jsonGenerator.writeString(field);
            }
            jsonGenerator.writeEndArray();
        }

        if (config.getTypeFields() != null) {
            jsonGenerator.writeArrayFieldStart("TypeFields");
            for (String field : config.getTypeFields()) {
                jsonGenerator.writeString(field);
            }
            jsonGenerator.writeEndArray();
        }

        jsonGenerator.writeStringField("ProxyMode", String.valueOf(config.isProxyMode()));

        jsonGenerator.writeEndObject();
    }

    private void serializeFcConfig(SinkFcConfig config, JsonGenerator jsonGenerator) throws IOException, JsonProcessingException {
        jsonGenerator.writeStartObject();

        jsonGenerator.writeStringField("Endpoint", config.getEndpoint());
        jsonGenerator.writeStringField("Service", config.getService());
        jsonGenerator.writeStringField("Function", config.getFunction());
        jsonGenerator.writeStringField("AuthMode", config.getAuthMode().name().toLowerCase());
        if (config.getAuthMode() == SinkConfig.AuthMode.AK) {
            jsonGenerator.writeStringField("AccessId", config.getAccessId());
            jsonGenerator.writeStringField("AccessKey", config.getAccessKey());
        }

        jsonGenerator.writeEndObject();
    }

    private void serializeOtsConfig(SinkOtsConfig config, JsonGenerator jsonGenerator) throws IOException, JsonProcessingException {
        jsonGenerator.writeStartObject();

        jsonGenerator.writeStringField("Endpoint", config.getEndpoint());
        jsonGenerator.writeStringField("InstanceName", config.getInstance());
        jsonGenerator.writeStringField("TableName", config.getTable());
        jsonGenerator.writeStringField("AuthMode", config.getAuthMode().name().toLowerCase());
        if (config.getAuthMode() == SinkConfig.AuthMode.AK) {
            jsonGenerator.writeStringField("AccessId", config.getAccessId());
            jsonGenerator.writeStringField("AccessKey", config.getAccessKey());
        }

        jsonGenerator.writeEndObject();
    }

    private void serializeOssConfig(SinkOssConfig config, JsonGenerator jsonGenerator) throws IOException, JsonProcessingException {
        jsonGenerator.writeStartObject();

        jsonGenerator.writeStringField("Endpoint", config.getEndpoint());
        jsonGenerator.writeStringField("Bucket", config.getBucket());
        jsonGenerator.writeStringField("Prefix", config.getPrefix());
        jsonGenerator.writeStringField("TimeFormat", config.getTimeFormat());
        jsonGenerator.writeNumberField("TimeRange", config.getTimeRange());
        jsonGenerator.writeStringField("AuthMode", config.getAuthMode().name().toLowerCase());
        if (config.getAuthMode() == SinkConfig.AuthMode.AK) {
            jsonGenerator.writeStringField("AccessId", config.getAccessId());
            jsonGenerator.writeStringField("AccessKey", config.getAccessKey());
        }

        jsonGenerator.writeEndObject();
    }
}