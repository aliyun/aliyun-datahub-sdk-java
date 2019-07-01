package com.aliyun.datahub.utils;


import com.aliyun.datahub.client.model.*;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.exception.InvalidParameterException;
import com.aliyun.datahub.model.RecordEntry;
import com.aliyun.datahub.model.*;

import java.util.Map;

public abstract class ModelConvertToOld {
    public static RecordSchema convertRecordSchema(com.aliyun.datahub.client.model.RecordSchema schema) {
        RecordSchema oldSchema = new RecordSchema();
        for (com.aliyun.datahub.client.model.Field field : schema.getFields()) {
            FieldType fieldType = FieldType.valueOf(field.getType().name().toUpperCase());
            Field oldField = new Field(field.getName(), fieldType, !field.isAllowNull());
            oldSchema.addField(oldField);
        }
        return oldSchema;
    }

    public static RecordEntry convertRecordEntry(com.aliyun.datahub.client.model.RecordEntry entry, long sequence) {
        com.aliyun.datahub.client.model.TupleRecordData data = (com.aliyun.datahub.client.model.TupleRecordData)entry.getRecordData();
        com.aliyun.datahub.client.model.RecordSchema newSchema = data.getRecordSchema();
        RecordSchema oldSchema = convertRecordSchema(newSchema);

        RecordEntry oldEntry = new RecordEntry(oldSchema);
        oldEntry.setShardId(entry.getShardId());
        oldEntry.setHashKey(entry.getHashKey());
        oldEntry.setPartitionKey(entry.getPartitionKey());
        oldEntry.setSystemTime(entry.getSystemTime());
        oldEntry.setSequence(entry.getSequence() == -1  ? sequence : entry.getSequence());
        if (entry.getAttributes() != null) {
            for (Map.Entry<String, String> strPair : entry.getAttributes().entrySet()) {
                oldEntry.putAttribute(strPair.getKey(), strPair.getValue());
            }
        }
        // data
        Object[] values = oldEntry.getValues();
        for (int i = 0; i < values.length; ++i) {
            values[i] = data.getField(i);
        }

        return oldEntry;
    }

    public static BlobRecordEntry convertBlobRecordEntry(com.aliyun.datahub.client.model.RecordEntry entry) {
        com.aliyun.datahub.client.model.BlobRecordData data = (com.aliyun.datahub.client.model.BlobRecordData)entry.getRecordData();
        BlobRecordEntry oldEntry = new BlobRecordEntry();
        oldEntry.setShardId(entry.getShardId());
        oldEntry.setHashKey(entry.getHashKey());
        oldEntry.setPartitionKey(entry.getPartitionKey());
        oldEntry.setSystemTime(entry.getSystemTime());
        oldEntry.setSequence(entry.getSequence());
        if (entry.getAttributes() != null) {
            for (Map.Entry<String, String> strPair : entry.getAttributes().entrySet()) {
                oldEntry.putAttribute(strPair.getKey(), strPair.getValue());
            }
        }
        // data
        oldEntry.setData(data.getData());
        return oldEntry;
    }

    public static ConnectorConfig convertConnectorConfig(SinkConfig config) {
        if (config == null) {
            throw new InvalidParameterException("config is null");
        }

        ConnectorConfig oldConfig = null;
        if (config instanceof SinkOdpsConfig) {
            oldConfig = convertOdpsConfig((SinkOdpsConfig)config);
        } else if (config instanceof SinkMysqlConfig) {
            oldConfig = convertDatabaseConfig((SinkMysqlConfig)config);
        } else if (config instanceof SinkDatahubConfig) {
            oldConfig = convertDatahubConfig((SinkDatahubConfig)config);
        } else if (config instanceof SinkEsConfig) {
            oldConfig = convertEsConfig((SinkEsConfig)config);
        } else if (config instanceof SinkFcConfig) {
            oldConfig = convertFcConfig((SinkFcConfig)config);
        } else if (config instanceof SinkOtsConfig) {
            oldConfig = convertOtsConfig((SinkOtsConfig)config);
        } else if (config instanceof SinkOssConfig) {
            oldConfig = convertOssConfig((SinkOssConfig)config);
        } else {
            throw new InvalidParameterException("config type is error");
        }

        return oldConfig;
    }

    //String project, String topic, String subId, String shardId, Offset offset, long version, String sessionId

    public static OffsetContext convertOffsetContext(
            String projectName, String topicName, String subId, String shardId,
            com.aliyun.datahub.client.model.SubscriptionOffset subOffset) {

        OffsetContext.Offset offset = new OffsetContext.Offset(subOffset.getSequence(), subOffset.getTimestamp());

        return new OffsetContext(projectName, topicName, subId, shardId, offset, subOffset.getVersionId(), subOffset.getSessionId());
    }


    private static ConnectorConfig convertOdpsConfig(SinkOdpsConfig config) {
        OdpsDesc.PartitionMode oldPartitionMode = null;
        if (config.getPartitionMode() != null) {
            oldPartitionMode = OdpsDesc.PartitionMode.valueOf(config.getPartitionMode().name().toUpperCase());
        }

        OdpsDesc desc = new OdpsDesc();
        desc.setOdpsEndpoint(config.getEndpoint());
        desc.setTunnelEndpoint(config.getTunnelEndpoint());
        desc.setProject(config.getProject());
        desc.setTable(config.getTable());
        desc.setPartitionMode(oldPartitionMode);
        desc.setTimeRange(config.getTimeRange());
        desc.setAccessId(config.getAccessId());
        desc.setAccessKey(config.getAccessKey());
        // partition config
        if (config.getPartitionConfig() != null) {
            desc.setPartitionConfig(config.getPartitionConfig().getConfigMap());
        }
        return desc;
    }

    private static ConnectorConfig convertDatabaseConfig(SinkMysqlConfig config) {
        DatabaseDesc desc = new DatabaseDesc();
        desc.setHost(config.getHost());
        desc.setPort(config.getPort());
        desc.setDatabase(config.getDatabase());
        desc.setTable(config.getTable());
        desc.setUser(config.getUser());
        desc.setPassword(config.getPassword());
        desc.setIgnore(config.getInsertMode() == SinkMysqlConfig.InsertMode.IGNORE);
        return desc;
    }

    private static ConnectorConfig convertDatahubConfig(SinkDatahubConfig config) {
        DatahubDesc desc = new DatahubDesc();
        desc.setEndpoint(config.getEndpoint());
        desc.setProject(config.getProjectName());
        desc.setTopic(config.getTopicName());
        return desc;
    }

    private static ConnectorConfig convertEsConfig(SinkEsConfig config) {
        ElasticSearchDesc desc = new ElasticSearchDesc();
        desc.setEndpoint(config.getEndpoint());
        desc.setIndex(config.getIndex());
        desc.setIdFields(config.getIdFields());
        desc.setTypeFields(config.getTypeFields());
        desc.setUser(config.getUser());
        desc.setPassword(config.getPassword());
        desc.setProxyMode(config.isProxyMode());

        return desc;
    }

    private static ConnectorConfig convertFcConfig(SinkFcConfig config) {
        FcDesc desc = new FcDesc();
        desc.setEndpoint(config.getEndpoint());
        desc.setService(config.getService());
        desc.setFunction(config.getFunction());
        desc.setAuthMode(config.getAuthMode().name());
        return desc;
    }

    private static ConnectorConfig convertOtsConfig(SinkOtsConfig config) {
        AuthMode oldAuthMode = null;
        if (config.getAuthMode() != null) {
            oldAuthMode = AuthMode.valueOf(config.getAuthMode().name().toUpperCase());
        }

        OtsDesc desc = new OtsDesc();
        desc.setEndpoint(config.getEndpoint());
        desc.setInstance(config.getInstance());
        desc.setTable(config.getTable());
        desc.setAuthMode(oldAuthMode);
        return desc;
    }

    private static ConnectorConfig convertOssConfig(SinkOssConfig config) {
        OssDesc desc = new OssDesc();
        desc.setEndpoint(config.getEndpoint());
        desc.setPrefix(config.getPrefix());
        desc.setBucket(config.getBucket());
        desc.setTimeFormat(config.getTimeFormat());
        desc.setTimeRange(config.getTimeRange());
        desc.setAuthMode(config.getAuthMode().name().toLowerCase());
        return desc;
    }

}
