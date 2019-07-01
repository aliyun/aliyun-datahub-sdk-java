package com.aliyun.datahub.client.impl;

import com.aliyun.datahub.client.auth.Account;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.exception.NoPermissionException;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.impl.request.*;
import com.aliyun.datahub.client.model.*;
import com.aliyun.datahub.client.util.FormatUtils;
import com.aliyun.datahub.client.util.KeyRangeUtils;
import com.codahale.metrics.Timer;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DatahubClientJsonImpl extends AbstractDatahubClient {
    public DatahubClientJsonImpl(String endpoint, Account account, HttpConfig httpConfig, String userAgent) {
        super(endpoint, account, httpConfig, userAgent);
    }

    @Override
    public GetProjectResult getProject(final String projectName) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }
        GetProjectResult result = callWrapper(getService().getProject(projectName));
        if (result != null) {
            result.setProjectName(projectName.toLowerCase());
        }
        return result;
    }

    @Override
    public ListProjectResult listProject() {
        return callWrapper(getService().listProject());
    }

    @Override
    public CreateProjectResult createProject(String projectName, String comment) {
        if (!FormatUtils.checkProjectName(projectName, true)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkComment(comment)) {
            throw new InvalidParameterException("Comment format is inivalid");
        }

        final CreateProjectRequest request = new CreateProjectRequest().setComment(comment);
        return callWrapper(getService().createProject(projectName, request));
    }

    @Override
    public UpdateProjectResult updateProject(String projectName, String comment) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkComment(comment)) {
            throw new InvalidParameterException("Comment format is inivalid");
        }

        final UpdateProjectRequest request = new UpdateProjectRequest().setComment(comment);
        return callWrapper(getService().updateProject(projectName, request));
    }

    @Override
    public DeleteProjectResult deleteProject(String projectName) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }
        return callWrapper(getService().deleteProject(projectName));
    }

    @Override
    public void waitForShardReady(String projectName, String topicName) {
        waitForShardReady(projectName, topicName, MAX_WAITING_TIME_IN_MS);
    }

    @Override
    public void waitForShardReady(String projectName, String topicName, long timeout) {
        if (timeout < 0) {
            throw new InvalidParameterException("Invalid timeout value: " + timeout);
        }

        timeout = Math.min(timeout, MAX_WAITING_TIME_IN_MS);
        long start = System.currentTimeMillis();
        long end = start + timeout;
        boolean allShardReady = false;
        while (start < end) {
            allShardReady = isAllShardReady(projectName, topicName);
            if (allShardReady) {
                break;
            }

            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                // nothing
            }
            start = System.currentTimeMillis();
        }

        if (!allShardReady) {
            throw new DatahubClientException("Wait loading shards timeout");
        }
    }

    private boolean isAllShardReady(String projectName, String topicName) {
        ListShardResult result = listShard(projectName, topicName);
        for (ShardEntry entry : result.getShards()) {
            if (ShardState.ACTIVE != entry.getState() && ShardState.CLOSED != entry.getState()) {
                return false;
            }
        }
        return true;
    }


    @Override
    public CreateTopicResult createTopic(String projectName, String topicName, int shardCount, int lifeCycle, RecordType recordType, String comment) {
        return createTopic(projectName, topicName, shardCount, lifeCycle, recordType, null, comment);
    }

    @Override
    public CreateTopicResult createTopic(String projectName, String topicName, int shardCount, int lifeCycle, RecordType recordType, RecordSchema recordSchema, String comment) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("Project name format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName, true)) {
            throw new InvalidParameterException("Topic name format is invalid");
        }

        if (shardCount <= 0 || lifeCycle <= 0) {
            throw new InvalidParameterException("ShardCoutn LifeCycle is invalid");
        }

        if (!FormatUtils.checkComment(comment)) {
            throw new InvalidParameterException("Comment format is inivalid");
        }

        if ((recordType == RecordType.TUPLE && recordSchema == null) ||
                (recordType == RecordType.BLOB && recordSchema != null)) {
            throw new InvalidParameterException("Record type is invalid");
        }

        final CreateTopicRequest request = new CreateTopicRequest()
                .setShardCount(shardCount)
                .setLifeCycle(lifeCycle)
                .setRecordType(recordType)
                .setRecordSchema(recordSchema)
                .setComment(comment);

        return callWrapper(getService().createTopic(projectName, topicName, request));
    }

    @Override
    public UpdateTopicResult updateTopic(String projectName, String topicName, String comment) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (!FormatUtils.checkComment(comment)) {
            throw new InvalidParameterException("Comment format is inivalid");
        }

        final UpdateTopicRequest request = new UpdateTopicRequest()
                .setComment(comment);
        return callWrapper(getService().updateTopic(projectName, topicName, request));
    }

    @Override
    public GetTopicResult getTopic(String projectName, String topicName) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        GetTopicResult result = callWrapper(getService().getTopic(projectName, topicName));
        if (result != null) {
            result.setProjectName(projectName);
            result.setTopicName(topicName);
        }
        return result;
    }

    @Override
    public DeleteTopicResult deleteTopic(String projectName, String topicName) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }
        return callWrapper(getService().deleteTopic(projectName, topicName));
    }

    @Override
    public ListTopicResult listTopic(String projectName) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }
        return callWrapper(getService().listTopic(projectName));
    }

    @Override
    public ListShardResult listShard(String projectName, String topicName) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        ListShardResult result = callWrapper(getService().listShard(projectName, topicName));
        if (result != null) {
            for (ShardEntry entry : result.getShards()) {
                if (MAX_SHARD_ID.equals(entry.getLeftShardId())) {
                    entry.setLeftShardId(null);
                }
                if (MAX_SHARD_ID.equals(entry.getRightShardId())) {
                    entry.setRightShardId(null);
                }
            }
        }
        return result;
    }

    @Override
    public SplitShardResult splitShard(String projectName, String topicName, String shardId) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (!FormatUtils.checkShardId(shardId)) {
            throw new InvalidParameterException("ShardId format is invalid");
        }

        String splitKey = getSplitKey(projectName, topicName, shardId);
        return splitShard(projectName, topicName, shardId, splitKey);
    }

    private String getSplitKey(String projectName, String topicName, String shardId) {
        String splitKey = null;
        ListShardResult originShards = listShard(projectName, topicName);
        for (ShardEntry entry : originShards.getShards()) {
            if (shardId.equalsIgnoreCase(entry.getShardId())) {
                if (entry.getState() != ShardState.ACTIVE) {
                    throw new NoPermissionException("Only active shard can be split");
                }

                try {
                    splitKey = KeyRangeUtils.trivialSplit(entry.getBeginHashKey(), entry.getEndHashKey());
                } catch (Exception e) {
                    throw new DatahubClientException(e.getMessage());
                }
            }
        }

        if (splitKey == null) {
            throw new DatahubClientException("Shard not exist");
        }

        return splitKey;
    }

    @Override
    public SplitShardResult splitShard(String projectName, String topicName, String shardId, String splitKey) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (!FormatUtils.checkShardId(shardId)) {
            throw new InvalidParameterException("ShardId format is invalid");
        }

        final SplitShardRequest request = new SplitShardRequest()
                .setShardId(shardId)
                .setSplitKey(splitKey);

        return callWrapper(getService().splitShard(projectName, topicName, request));
    }

    @Override
    public MergeShardResult mergeShard(String projectName, String topicName, String shardId, String adjacentShardId) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (!FormatUtils.checkShardId(shardId) || !FormatUtils.checkShardId(adjacentShardId)) {
            throw new InvalidParameterException("ShardId format is invalid");
        }

        final MergeShardRequest request = new MergeShardRequest()
                .setShardId(shardId)
                .setAdjacentShardId(adjacentShardId);
        return callWrapper(getService().mergeShard(projectName, topicName, request));
    }

    @Override
    public GetCursorResult getCursor(String projectName, String topicName, String shardId, CursorType type) {
        return getCursor(projectName, topicName, shardId, type, -1);
    }

    @Override
    public GetCursorResult getCursor(String projectName, String topicName, String shardId, CursorType type, long parameter) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (!FormatUtils.checkShardId(shardId)) {
            throw new InvalidParameterException("ShardId format is invalid");
        }

        if (type == null) {
            throw new InvalidParameterException("Type is null");
        }

        if ((type == CursorType.SEQUENCE || type == CursorType.SYSTEM_TIME) && parameter == -1) {
            throw new InvalidParameterException("Cursor type or parameter is invalid");
        }

        final GetCursorRequest request = new GetCursorRequest()
                .setType(type)
                .setParameter(parameter);
        return callWrapper(getService().getCursor(projectName, topicName, shardId, request));
    }

    @Override
    public PutRecordsResult putRecords(String projectName, String topicName, final List<RecordEntry> records) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (records == null || records.isEmpty()) {
            throw new InvalidParameterException("Records is null or empty");
        }

        final PutRecordsRequest request = new PutRecordsRequest().setRecords(records);

        Timer.Context context = PUT_LATENCY_TIMER == null ? null : PUT_LATENCY_TIMER.time();
        try {
            PutRecordsResult result = callWrapper(getService().putRecords(projectName, topicName, request));
            if (result != null) {
                if (result.getFailedRecordCount() > 0) {
                    List<RecordEntry> failedRecords = new ArrayList<>();
                    for (PutErrorEntry errorEntry : result.getPutErrorEntries()) {
                        failedRecords.add(request.getRecords().get(errorEntry.getIndex()));
                    }
                    result.setFailedRecords(failedRecords);
                }
                if (PUT_QPS_METER != null) {
                    PUT_QPS_METER.mark(1);
                }

                if (PUT_RPS_METER != null) {
                    PUT_RPS_METER.mark(records.size() - result.getFailedRecordCount());
                }
            }
            return result;
        } finally {
            if (context != null) {
                context.stop();
            }
        }
    }

    @Override
    public PutRecordsByShardResult putRecordsByShard(String projectName, String topicName, String shardId, List<RecordEntry> records) {
        throw new DatahubClientException("This method is not supported");
    }

    @Override
    public GetRecordsResult getRecords(String projectName, String topicName, final String shardId, String cursor, int limit) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (StringUtils.isEmpty(cursor)) {
            throw new InvalidParameterException("Cursor format is invalid");
        }

        limit = Math.max(MIN_FETCH_SIZE, limit);
        limit = Math.min(MAX_FETCH_SIZE, limit);

        final GetRecordsRequest request = new GetRecordsRequest().setCursor(cursor).setLimit(limit);

        final Timer.Context context = GET_LATENCY_TIMER == null ? null : GET_LATENCY_TIMER.time();
        try {
            GetRecordsResult result = callWrapper(getService().getRecords(projectName, topicName, shardId, request));
            if (result != null) {
                if (result.getRecordCount() > 0) {
                    long recordIndex = 0;
                    for (RecordEntry entry : result.getRecords()) {
                        long sequence = result.getStartSequence() + recordIndex++;
                        entry.setShardId(shardId);
                        entry.setSequence(entry.getSequence() == -1 ? sequence : entry.getSequence());
                    }
                }
                if (GET_QPS_METER != null) {
                    GET_QPS_METER.mark(1);
                }

                if (GET_RPS_METER != null) {
                    GET_RPS_METER.mark(result.getRecordCount());
                }
            }
            return result;
        } finally {
            if (context != null) {
                context.stop();
            }
        }
    }

    @Override
    public GetRecordsResult getRecords(String projectName, String topicName, String shardId, RecordSchema schema, String cursor, int limit) {
        if (schema == null) {
            throw new InvalidParameterException("Record schema is null");
        }

        GetRecordsResult result = getRecords(projectName, topicName, shardId, cursor, limit);
        for (RecordEntry entry : result.getRecords()) {
            if (entry.getRecordData() == null || !(entry.getRecordData() instanceof TupleRecordData)) {
                throw new DatahubClientException("Shouldn't call this method for BLOB topic");
            }
            ((TupleRecordData)entry.getRecordData()).internalConvertAuxValues(schema);
        }
        return result;
    }

    @Override
    public AppendFieldResult appendField(String projectName, String topicName, Field field) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (field == null || !field.isAllowNull()) {
            throw new InvalidParameterException("append field must allow null value");
        }

        final AppendFieldRequest request = new AppendFieldRequest().setFieldName(field.getName()).setFieldType(field.getType());
        return callWrapper(getService().appendField(projectName, topicName, request));
    }

    @Override
    public GetMeterInfoResult getMeterInfo(String projectName, String topicName, String shardId) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (!FormatUtils.checkShardId(shardId)) {
            throw new InvalidParameterException("ShardId format is invalid");
        }

        final GetMeterInfoRequest request = new GetMeterInfoRequest();
        return callWrapper(getService().getMeterInfo(projectName, topicName, shardId, request));
    }

    @Override
    public CreateConnectorResult createConnector(String projectName, String topicName, ConnectorType connectorType, List<String> columnFields, SinkConfig config) {
        return createConnector(projectName, topicName, connectorType, -1, columnFields, config);
    }

    @Override
    public CreateConnectorResult createConnector(String projectName, String topicName, ConnectorType connectorType, long sinkStartTime, List<String> columnFields, SinkConfig config) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (config == null) {
            throw new InvalidParameterException("Config is null");
        }

        final CreateConnectorRequest request = new CreateConnectorRequest()
                .setSinkStartTime(sinkStartTime)
                .setColumnFields(columnFields)
                .setType(connectorType)
                .setConfig(config);
        return callWrapper(getService().createConnector(projectName, topicName,
                connectorType.name().toLowerCase(), request));
    }

    @Override
    public GetConnectorResult getConnector(String projectName, String topicName, ConnectorType connectorType) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (connectorType == null) {
            throw new InvalidParameterException("ConnectorType is null");
        }

        return callWrapper(getService().getConnector(projectName, topicName, connectorType.name().toLowerCase()));
    }

    @Override
    public UpdateConnectorResult updateConnector(String projectName, String topicName, ConnectorType connectorType, SinkConfig config) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (connectorType == null) {
            throw new InvalidParameterException("ConnectorType is null");
        }

        final UpdateConnectorRequest request = new UpdateConnectorRequest().setConfig(config);
        return callWrapper(getService().updateConnector(projectName, topicName,
                connectorType.name().toLowerCase(), request));
    }

    @Override
    public ListConnectorResult listConnector(String projectName, String topicName) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        return callWrapper(getService().listConnector(projectName, topicName));
    }

    @Override
    public DeleteConnectorResult deleteConnector(String projectName, String topicName, ConnectorType connectorType) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (connectorType == null) {
            throw new InvalidParameterException("ConnectorType is null");
        }

        return callWrapper(getService().deleteConnector(projectName, topicName, connectorType.name().toLowerCase()));
    }

    @Override
    public GetConnectorDoneTimeResult getConnectorDoneTime(String projectName, String topicName, ConnectorType connectorType) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (ConnectorType.SINK_ODPS != connectorType) {
            throw new InvalidParameterException("Only sink maxcompute supports done time");
        }

        return callWrapper(getService().getConnectorDoneTime(projectName, topicName, connectorType.name().toLowerCase()));
    }

    @Override
    public ReloadConnectorResult reloadConnector(String projectName, String topicName, ConnectorType connectorType) {
        return reloadConnector(projectName, topicName, connectorType, null);
    }

    @Override
    public ReloadConnectorResult reloadConnector(String projectName, String topicName, ConnectorType connectorType, String shardId) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (connectorType == null) {
            throw new InvalidParameterException("ConnectorType is null");
        }

        final ReloadConnectorRequest request = new ReloadConnectorRequest().setShardId(shardId);
        return callWrapper(getService().reloadConnector(projectName, topicName,
                connectorType.name().toLowerCase(), request));
    }

    @Override
    public UpdateConnectorStateResult updateConnectorState(String projectName, String topicName, ConnectorType connectorType, ConnectorState connectorState) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("topicName format is invalid");
        }

        if (ConnectorState.CREATED == connectorState) {
            throw new InvalidParameterException("connectorState is invalid");
        }

        final UpdateConnectorStateRequest request = new UpdateConnectorStateRequest().setState(connectorState);
        return callWrapper(getService().updateConnectorState(projectName, topicName,
                connectorType.name().toLowerCase(), request));
    }

    @Override
    public UpdateConnectorOffsetResult updateConnectorOffset(String projectName, String topicName, ConnectorType connectorType, String shardId, ConnectorOffset offset) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("topicName format is invalid");
        }

        if (shardId != null && !FormatUtils.checkShardId(shardId)) {
            throw new InvalidParameterException("ShardId format is invalid");
        }

        final UpdateConnectorOffsetRequest request = new UpdateConnectorOffsetRequest().setShardId(shardId)
                .setTimestamp(offset.getTimestamp()).setSequence(offset.getSequence());
        return callWrapper(getService().updateConnectorOffset(projectName, topicName,
                connectorType.name().toLowerCase(), request));
    }

    @Override
    public GetConnectorShardStatusResult getConnectorShardStatus(String projectName, String topicName, ConnectorType connectorType) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (connectorType == null) {
            throw new InvalidParameterException("ConnectorType is null");
        }

        final GetConnectorShardStatusRequest request = new GetConnectorShardStatusRequest();
        return callWrapper(getService().getConnectorShardStatus(projectName, topicName,
                connectorType.name().toLowerCase(), request));
    }

    @Override
    public ConnectorShardStatusEntry getConnectorShardStatus(String projectName, String topicName, ConnectorType connectorType, String shardId) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (connectorType == null) {
            throw new InvalidParameterException("ConnectorType is null");
        }

        if (!FormatUtils.checkShardId(shardId)) {
            throw new InvalidParameterException("ShardId format is invalid");
        }

        final GetConnectorShardStatusRequest request = new GetConnectorShardStatusRequest().setShardId(shardId);
        return callWrapper(getService().getConnectorShardStatusByShard(projectName, topicName,
                connectorType.name().toLowerCase(), request));
    }

    @Override
    public AppendConnectorFieldResult appendConnectorField(String projectName, String topicName, ConnectorType connectorType, String fieldName) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (connectorType == null) {
            throw new InvalidParameterException("ConnectorType is null");
        }

        if (StringUtils.isEmpty(fieldName)) {
            throw new InvalidParameterException("FieldName is invalid");
        }

        final AppendConnectorFieldRequest request = new AppendConnectorFieldRequest().setFieldName(fieldName.toLowerCase());
        return callWrapper(getService().appendConnectorField(projectName, topicName,
                connectorType.name().toLowerCase(), request));
    }

    @Override
    public CreateSubscriptionResult createSubscription(String projectName, String topicName, String comment) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (!FormatUtils.checkComment(comment)) {
            throw new InvalidParameterException("Comment format is invalid");
        }

        final CreateSubscriptionRequest request = new CreateSubscriptionRequest().setComment(comment);
        return callWrapper(getService().createSubscription(projectName, topicName, request));
    }

    @Override
    public GetSubscriptionResult getSubscription(String projectName, String topicName, String subId) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (StringUtils.isEmpty(subId)) {
            throw new InvalidParameterException("SubId format is invalid");
        }

        return callWrapper(getService().getSubscription(projectName, topicName, subId));
    }

    @Override
    public DeleteSubscriptionResult deleteSubscription(String projectName, String topicName, String subId) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (StringUtils.isEmpty(subId)) {
            throw new InvalidParameterException("SubId format is invalid");
        }
        return callWrapper(getService().deleteSubscription(projectName, topicName, subId));
    }

    @Override
    public ListSubscriptionResult listSubscription(String projectName, String topicName, int pageNum, int pageSize) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (pageNum <= 0 || pageSize <= 0) {
            throw new InvalidParameterException("Page format is invalid");
        }

        final ListSubscriptionRequest request = new ListSubscriptionRequest()
                .setPageNum(pageNum)
                .setPageSize(pageSize);
        return callWrapper(getService().listSubscription(projectName, topicName, request));
    }

    @Override
    public UpdateSubscriptionResult updateSubscription(String projectName, String topicName, String subId, String comment) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (StringUtils.isEmpty(subId)) {
            throw new InvalidParameterException("SubId format is invalid");
        }

        if (!FormatUtils.checkComment(comment)) {
            throw new InvalidParameterException("Comment format is invalid");
        }

        final UpdateSubscriptionRequest request = new UpdateSubscriptionRequest()
                .setComment(comment);
        return callWrapper(getService().updateSubscription(projectName, topicName,
                subId, request));
    }

    @Override
    public UpdateSubscriptionStateResult updateSubscriptionState(String projectName, String topicName, String subId, SubscriptionState state) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (StringUtils.isEmpty(subId)) {
            throw new InvalidParameterException("SubId format is invalid");
        }

        if (state == null) {
            throw new InvalidParameterException("State is null");
        }

        final UpdateSubscriptionRequest request = new UpdateSubscriptionRequest()
                .setState(state);
        return callWrapper(getService().updateSubscriptionState(projectName, topicName, subId, request));
    }

    @Override
    public OpenSubscriptionSessionResult openSubscriptionSession(String projectName, String topicName, String subId, List<String> shardIds) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (StringUtils.isEmpty(subId)) {
            throw new InvalidParameterException("SubId format is invalid");
        }

        if (shardIds == null || shardIds.isEmpty()) {
            throw new InvalidParameterException("ShardIds is null");
        }

        final OpenSubscriptionSessionRequest request = new OpenSubscriptionSessionRequest().setShardIds(shardIds);
        return callWrapper(getService().openSubscriptionSession(projectName, topicName, subId, request));
    }

    @Override
    public GetSubscriptionOffsetResult getSubscriptionOffset(String projectName, String topicName, String subId, List<String> shardIds) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (StringUtils.isEmpty(subId)) {
            throw new InvalidParameterException("SubId format is invalid");
        }

        final GetSubscriptionOffsetRequest request = new GetSubscriptionOffsetRequest().setShardIds(shardIds);
        return callWrapper(getService().getSubscriptionOffset(projectName, topicName, subId, request));
    }

    @Override
    public CommitSubscriptionOffsetResult commitSubscriptionOffset(String projectName, String topicName, String subId, Map<String, SubscriptionOffset> offsets) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (StringUtils.isEmpty(subId)) {
            throw new InvalidParameterException("SubId format is invalid");
        }

        if (offsets == null || offsets.isEmpty()) {
            throw new InvalidParameterException("Offsets is null");
        }

        final CommitSubscriptionOffsetRequest request = new CommitSubscriptionOffsetRequest().setOffsets(offsets);
        return callWrapper(getService().commitSubscriptionOffset(projectName, topicName, subId, request));
    }

    @Override
    public ResetSubscriptionOffsetResult resetSubscriptionOffset(String projectName, String topicName, String subId, Map<String, SubscriptionOffset> offsets) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (StringUtils.isEmpty(subId)) {
            throw new InvalidParameterException("SubId format is invalid");
        }

        if (offsets == null || offsets.isEmpty()) {
            throw new InvalidParameterException("Offsets is null");
        }

        final ResetSubscriptionOffsetRequest request = new ResetSubscriptionOffsetRequest().setOffsets(offsets);
        return callWrapper(getService().resetSubscriptionOffset(projectName, topicName, subId, request));
    }

    @Override
    public HeartbeatResult heartbeat(String projectName, String topicName, String consumerGroup, String consumerId, long versionId, List<String> holdShardList, List<String> readEndShardList) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (StringUtils.isEmpty(consumerGroup)) {
            throw new InvalidParameterException("ConsumerGroup format is invalid");
        }

        if (holdShardList == null) {
            throw new InvalidParameterException("HoldShardList is null");
        }

        final HeartbeatRequest request = new HeartbeatRequest().setConsumerId(consumerId).setVersionId(versionId)
                .setHoldShardList(holdShardList).setReadEndShardList(readEndShardList);
        return callWrapper(getService().heartbeat(projectName, topicName, consumerGroup, request));
    }

    @Override
    public JoinGroupResult joinGroup(String projectName, String topicName, String consumerGroup, long sessionTimeout) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (StringUtils.isEmpty(consumerGroup)) {
            throw new InvalidParameterException("ConsumerGroup format is invalid");
        }

        final JoinGroupRequest request = new JoinGroupRequest().setSessionTimeout(sessionTimeout);
        return callWrapper(getService().joinGroup(projectName, topicName, consumerGroup, request));
    }

    @Override
    public SyncGroupResult syncGroup(String projectName, String topicName, String consumerGroup, String consumerId, long versionId, List<String> releaseShardList, List<String> readEndShardList) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (StringUtils.isEmpty(consumerGroup)) {
            throw new InvalidParameterException("ConsumerGroup format is invalid");
        }
        final SyncGroupRequest request = new SyncGroupRequest().setConsumerId(consumerId).setVersionId(versionId)
                .setReleaseShardList(releaseShardList).setReadEndShardList(readEndShardList);
        return callWrapper(getService().syncGroup(projectName, topicName, consumerGroup, request));
    }

    @Override
    public LeaveGroupResult leaveGroup(String projectName, String topicName, String consumerGroup, String consumerId, long versionId) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (StringUtils.isEmpty(consumerGroup)) {
            throw new InvalidParameterException("ConsumerGroup format is invalid");
        }
        final LeaveGroupRequest request = new LeaveGroupRequest().setConsumerId(consumerId).setVersionId(versionId);
        return callWrapper(getService().leaveGroup(projectName, topicName, consumerGroup, request));
    }
}
