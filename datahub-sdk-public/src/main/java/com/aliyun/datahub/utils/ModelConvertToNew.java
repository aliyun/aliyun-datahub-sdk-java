package com.aliyun.datahub.utils;


import com.aliyun.datahub.client.model.*;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.exception.InvalidParameterException;
import com.aliyun.datahub.model.RecordEntry;
import com.aliyun.datahub.model.*;

import java.util.Map;

public abstract class ModelConvertToNew {

    public static com.aliyun.datahub.client.model.RecordSchema convertRecordSchema(RecordSchema schema) {
        if (schema == null) {
            throw new InvalidParameterException("schema is null");
        }

        com.aliyun.datahub.client.model.RecordSchema retSchema = new com.aliyun.datahub.client.model.RecordSchema();
        for (Field field : schema.getFields()) {
            com.aliyun.datahub.client.model.FieldType newType = com.aliyun.datahub.client.model.FieldType.valueOf(field.getType().name().toUpperCase());
            com.aliyun.datahub.client.model.Field newField = new com.aliyun.datahub.client.model.Field(field.getName(), newType, !field.getNotnull());

            retSchema.addField(newField);
        }

        return retSchema;
    }

    public static com.aliyun.datahub.client.model.RecordEntry convertRecordEntry(RecordEntry entry) {
        if (entry == null || entry.getSchema() == null) {
            throw new InvalidParameterException("tuple record entry schema is null");
        }

        com.aliyun.datahub.client.model.RecordSchema newSchema = convertRecordSchema(entry.getSchema());
        com.aliyun.datahub.client.model.RecordEntry newEntry = new com.aliyun.datahub.client.model.RecordEntry();

        newEntry.setShardId(entry.getShardId());
        newEntry.setHashKey(entry.getHashKey());
        newEntry.setPartitionKey(entry.getPartitionKey());
        newEntry.setAttributes(entry.getAttributes());
        newEntry.setSystemTime(entry.getSystemTime());
        newEntry.setSequence(entry.getSequence());

        com.aliyun.datahub.client.model.TupleRecordData data = new com.aliyun.datahub.client.model.TupleRecordData(newSchema);
        for (int i = 0; i < entry.getFieldCount(); ++i) {
            data.setField(i, entry.get(i));
        }
        newEntry.setRecordData(data);
        return newEntry;
    }

    public static com.aliyun.datahub.client.model.RecordEntry convertBlobRecordEntry(BlobRecordEntry entry) {
        if (entry == null) {
            throw new InvalidParameterException("blob entry is null");
        }

        com.aliyun.datahub.client.model.RecordEntry newEntry = new com.aliyun.datahub.client.model.RecordEntry();

        newEntry.setShardId(entry.getShardId());
        newEntry.setHashKey(entry.getHashKey());
        newEntry.setPartitionKey(entry.getPartitionKey());
        newEntry.setAttributes(entry.getAttributes());
        newEntry.setSystemTime(entry.getSystemTime());
        newEntry.setSequence(entry.getSequence());

        com.aliyun.datahub.client.model.BlobRecordData data = new com.aliyun.datahub.client.model.BlobRecordData(entry.getData());
        newEntry.setRecordData(data);
        return newEntry;
    }

    public static SinkConfig convertConnectorConfig(ConnectorConfig config) {
        if (config == null) {
            throw new InvalidParameterException("config is null");
        }

        SinkConfig newConfig = null;
        if (config instanceof OdpsDesc) {
            newConfig = convertOdpsConfig((OdpsDesc)config);
        } else if (config instanceof DatabaseDesc) {
            newConfig = convertDatabaseConfig((DatabaseDesc)config);
        } else if (config instanceof DatahubDesc) {
            newConfig = convertDatahubConfig((DatahubDesc)config);
        } else if (config instanceof ElasticSearchDesc) {
            newConfig = convertEsConfig((ElasticSearchDesc)config);
        } else if (config instanceof FcDesc) {
            newConfig = convertFcConfig((FcDesc)config);
        } else if (config instanceof OtsDesc) {
            newConfig = convertOtsConfig((OtsDesc)config);
        } else if (config instanceof OssDesc) {
            newConfig = convertOssConfig((OssDesc)config);
        } else {
            throw new InvalidParameterException("config type is error");
        }

        return newConfig;
    }

    public static com.aliyun.datahub.client.model.SubscriptionOffset convertOffsetContext(OffsetContext context) {
        com.aliyun.datahub.client.model.SubscriptionOffset subOffset = new com.aliyun.datahub.client.model.SubscriptionOffset();
        subOffset.setSequence(context.getOffset().getSequence());
        subOffset.setTimestamp(context.getOffset().getTimestamp());
        subOffset.setVersionId(context.getVersion());
        subOffset.setSessionId(context.getSessionId());
        return subOffset;
    }


    private static SinkConfig convertOdpsConfig(OdpsDesc config) {
        SinkOdpsConfig newConfig = new SinkOdpsConfig();
        newConfig.setEndpoint(config.getOdpsEndpoint());
        newConfig.setTunnelEndpoint(config.getTunnelEndpoint());
        newConfig.setProject(config.getProject());
        newConfig.setTable(config.getTable());
        newConfig.setTimeRange(config.getTimeRange());
        newConfig.setPartitionMode(config.getPartitionMode() == null ?
                SinkOdpsConfig.PartitionMode.USER_DEFINE : SinkOdpsConfig.PartitionMode.valueOf(config.getPartitionMode().name().toUpperCase()));
        newConfig.setAccessId(config.getAccessId());
        newConfig.setAccessKey(config.getAccessKey());
        // partition config
        if (config.getPartitionConfig() == null) {
            // set empty config for for compatibility.
            newConfig.setPartitionConfig(new SinkOdpsConfig.PartitionConfig());
        } else {
            SinkOdpsConfig.PartitionConfig partitionConfig = new SinkOdpsConfig.PartitionConfig();
            for (Map.Entry<String, String> strPair : config.getPartitionConfig().entrySet()) {
                partitionConfig.addConfig(strPair.getKey(), strPair.getValue());
            }
            newConfig.setPartitionConfig(partitionConfig);
        }

        return newConfig;
    }

    private static SinkConfig convertDatabaseConfig(DatabaseDesc config) {
        SinkMysqlConfig newConfig = new SinkMysqlConfig();
        newConfig.setHost(config.getHost());
        newConfig.setPort(config.getPort() == null ? 0 : config.getPort());
        newConfig.setDatabase(config.getDatabase());
        newConfig.setTable(config.getTable());
        newConfig.setUser(config.getUser());
        newConfig.setPassword(config.getPassword());
        newConfig.setInsertMode(config.isIgnore() ? SinkMysqlConfig.InsertMode.IGNORE : SinkMysqlConfig.InsertMode.OVERWRITE);
        return newConfig;
    }

    private static SinkConfig convertDatahubConfig(DatahubDesc config) {
        SinkDatahubConfig newConfig = new SinkDatahubConfig();
        newConfig.setEndpoint(config.getEndpoint());
        newConfig.setProjectName(config.getProject());
        newConfig.setTopicName(config.getTopic());
        return newConfig;
    }

    private static SinkConfig convertEsConfig(ElasticSearchDesc config) {
        SinkEsConfig newConfig = new SinkEsConfig();
        newConfig.setEndpoint(config.getEndpoint());
        newConfig.setIndex(config.getIndex());
        newConfig.setTypeFields(config.getTypeFields());
        newConfig.setIdFields(config.getIdFields());
        newConfig.setUser(config.getUser());
        newConfig.setPassword(config.getPassword());
        newConfig.setProxyMode(config.isProxyMode());
        return newConfig;
    }

    private static SinkConfig convertFcConfig(FcDesc config) {
        SinkConfig.AuthMode newAuthMode = null;
        if (config.getAuthMode() != null) {
            newAuthMode = SinkConfig.AuthMode.valueOf(config.getAuthMode().toUpperCase());
        }

        com.aliyun.datahub.client.model.CursorType newStartPosition = null;
        if (config.getStartPosition() != null) {
            newStartPosition = com.aliyun.datahub.client.model.CursorType.valueOf(config.getStartPosition().toUpperCase());
        }

        SinkFcConfig newConfig = new SinkFcConfig();
        newConfig.setEndpoint(config.getEndpoint());
        newConfig.setService(config.getService());
        newConfig.setFunction(config.getFunction());
        newConfig.setAuthMode(newAuthMode);
        newConfig.setAccessId(config.getAccessId());
        newConfig.setAccessKey(config.getAccessKey());
        return newConfig;
    }


    private static SinkConfig convertOtsConfig(OtsDesc config) {
        SinkOtsConfig newConfig = new SinkOtsConfig();
        SinkConfig.AuthMode newAuthMode = null;
        if (config.getAuthMode() != null) {
            newAuthMode = SinkConfig.AuthMode.valueOf(config.getAuthMode().name().toUpperCase());
        }

        newConfig.setEndpoint(config.getEndpoint());
        newConfig.setTable(config.getTable());
        newConfig.setInstance(config.getInstance());
        newConfig.setAuthMode(newAuthMode);
        newConfig.setAccessId(config.getAccessId());
        newConfig.setAccessKey(config.getAccessKey());
        return newConfig;
    }

    private static SinkConfig convertOssConfig(OssDesc config) {
        SinkOssConfig newConfig = new SinkOssConfig();
        SinkConfig.AuthMode newAuthMode = null;
        if (config.getAuthMode() != null) {
            newAuthMode = SinkConfig.AuthMode.valueOf(config.getAuthMode().toUpperCase());
        }

        newConfig.setEndpoint(config.getEndpoint());
        newConfig.setPrefix(config.getPrefix());
        newConfig.setBucket(config.getBucket());
        newConfig.setTimeRange(config.getTimeRange());
        newConfig.setTimeFormat(config.getTimeFormat());
        newConfig.setAuthMode(newAuthMode);
        newConfig.setAuthMode(SinkConfig.AuthMode.valueOf(config.getAuthMode().toUpperCase()));
        newConfig.setAccessId(config.getAccessId());
        newConfig.setAccessKey(config.getAccessKey());
        return newConfig;
    }


}
