package com.aliyun.datahub.client.impl;

import com.aliyun.datahub.client.auth.Account;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.exception.NoPermissionException;
import com.aliyun.datahub.client.http.Entity;
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
import java.util.concurrent.Callable;

import static com.aliyun.datahub.client.common.DatahubConstant.*;

public class DatahubClientJsonImpl extends AbstractDatahubClient {
    public DatahubClientJsonImpl(String endpoint, Account account, HttpConfig httpConfig) {
        super(endpoint, account, httpConfig);
    }

    @Override
    public GetProjectResult getProject(final String projectName) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        final String path = String.format(PROJECT_URL, projectName);
        return callWrapper(new Callable<GetProjectResult>() {
            @Override
            public GetProjectResult call() throws Exception {
                GetProjectResult result = createRequest().path(path).get(GetProjectResult.class);
                result.setProjectName(projectName.toLowerCase());
                return result;
            }
        });
    }

    @Override
    public ListProjectResult listProject() {
        return callWrapper(new Callable<ListProjectResult>() {
            @Override
            public ListProjectResult call() throws Exception {
                return createRequest().path(PROJECTS_URL).get(ListProjectResult.class);
            }
        });
    }

    @Override
    public CreateProjectResult createProject(String projectName, String comment) {
        if (!FormatUtils.checkProjectName(projectName, true)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkComment(comment)) {
            throw new InvalidParameterException("Comment format is inivalid");
        }

        final String path = String.format(PROJECT_URL, projectName.toLowerCase());
        final CreateProjectRequest request = new CreateProjectRequest().setComment(comment);
        return callWrapper(new Callable<CreateProjectResult>() {
            @Override
            public CreateProjectResult call() throws Exception {
                return createRequest().path(path)
                        .post(Entity.json(request), CreateProjectResult.class);
            }
        });
    }

    @Override
    public UpdateProjectResult updateProject(String projectName, String comment) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkComment(comment)) {
            throw new InvalidParameterException("Comment format is inivalid");
        }

        final String path = String.format(PROJECT_URL, projectName);
        final UpdateProjectRequest request = new UpdateProjectRequest().setComment(comment);
        return callWrapper(new Callable<UpdateProjectResult>() {
            @Override
            public UpdateProjectResult call() throws Exception {
                return createRequest().path(path).put(Entity.json(request), UpdateProjectResult.class);
            }
        });
    }

    @Override
    public DeleteProjectResult deleteProject(String projectName) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        final String path = String.format(PROJECT_URL, projectName);
        return callWrapper(new Callable<DeleteProjectResult>() {
            @Override
            public DeleteProjectResult call() throws Exception {
                return createRequest().path(path).delete(DeleteProjectResult.class);
            }
        });
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

        final String path = String.format(TOPIC_URL, projectName.toLowerCase(), topicName.toLowerCase());
        final CreateTopicRequest request = new CreateTopicRequest()
                .setShardCount(shardCount)
                .setLifeCycle(lifeCycle)
                .setRecordType(recordType)
                .setRecordSchema(recordSchema)
                .setComment(comment);

        return callWrapper(new Callable<CreateTopicResult>() {
            @Override
            public CreateTopicResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), CreateTopicResult.class);
            }
        });
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

        final String path = String.format(TOPIC_URL, projectName, topicName);
        final UpdateTopicRequest request = new UpdateTopicRequest()
                .setComment(comment);
        return callWrapper(new Callable<UpdateTopicResult>() {
            @Override
            public UpdateTopicResult call() throws Exception {
                return createRequest().path(path).put(Entity.json(request), UpdateTopicResult.class);
            }
        });
    }

    @Override
    public GetTopicResult getTopic(String projectName, String topicName) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        final String fProject = projectName;
        final String fTopic = topicName;
        final String path = String.format(TOPIC_URL, projectName, topicName);
        return callWrapper(new Callable<GetTopicResult>() {
            @Override
            public GetTopicResult call() throws Exception {
                GetTopicResult result =
                        createRequest().path(path).get(GetTopicResult.class);
                result.setProjectName(fProject);
                result.setTopicName(fTopic);
                return result;
            }
        });
    }

    @Override
    public DeleteTopicResult deleteTopic(String projectName, String topicName) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        final String path = String.format(TOPIC_URL, projectName, topicName);
        return callWrapper(new Callable<DeleteTopicResult>() {
            @Override
            public DeleteTopicResult call() throws Exception {
                return createRequest().path(path).delete(DeleteTopicResult.class);
            }
        });
    }

    @Override
    public ListTopicResult listTopic(String projectName) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        final String path = String.format(TOPICS_URL, projectName);
        return callWrapper(new Callable<ListTopicResult>() {
            @Override
            public ListTopicResult call() throws Exception {
                return createRequest().path(path).get(ListTopicResult.class);
            }
        });
    }

    @Override
    public ListShardResult listShard(String projectName, String topicName) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        final String path = String.format(SHARDS_URL, projectName, topicName);
        return callWrapper(new Callable<ListShardResult>() {
            @Override
            public ListShardResult call() throws Exception {
                ListShardResult result = createRequest().path(path).get(ListShardResult.class);
                // filter as api response is not accurate
                for (ShardEntry entry : result.getShards()) {
                    if (MAX_SHARD_ID.equals(entry.getLeftShardId())) {
                        entry.setLeftShardId(null);
                    }
                    if (MAX_SHARD_ID.equals(entry.getRightShardId())) {
                        entry.setRightShardId(null);
                    }
                }
                return result;
            }
        });
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

        final String path = String.format(SHARDS_URL, projectName, topicName);
        return callWrapper(new Callable<SplitShardResult>() {
            @Override
            public SplitShardResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), SplitShardResult.class);
            }
        });
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

        final String path = String.format(SHARDS_URL, projectName, topicName);
        return callWrapper(new Callable<MergeShardResult>() {
            @Override
            public MergeShardResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), MergeShardResult.class);
            }
        });
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

        final String path = String.format(SHARD_URL, projectName, topicName, shardId);
        final GetCursorRequest request = new GetCursorRequest()
                .setType(type)
                .setParameter(parameter);

        return callWrapper(new Callable<GetCursorResult>() {
            @Override
            public GetCursorResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), GetCursorResult.class);
            }
        });
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

        final String path = String.format(SHARDS_URL, projectName, topicName);
        final PutRecordsRequest request = new PutRecordsRequest().setRecords(records);

        return callWrapper(new Callable<PutRecordsResult>() {
            @Override
            public PutRecordsResult call() throws Exception {
                Timer.Context context = PUT_LATENCY_TIMER == null ? null : PUT_LATENCY_TIMER.time();
                try {
                    PutRecordsResult result = createRequest().path(path).post(Entity.json(request), PutRecordsResult.class);
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
                    return result;
                } finally {
                    if (context != null) {
                        context.stop();
                    }
                }
            }
        });
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

        final String path = String.format(SHARD_URL, projectName, topicName, shardId);
        final GetRecordsRequest request = new GetRecordsRequest().setCursor(cursor).setLimit(limit);

        return callWrapper(new Callable<GetRecordsResult>() {
            @Override
            public GetRecordsResult call() throws Exception {
                final Timer.Context context = GET_LATENCY_TIMER == null ? null : GET_LATENCY_TIMER.time();
                try {
                    GetRecordsResult result = createRequest().path(path).post(Entity.json(request), GetRecordsResult.class);
                    if (result.getRecordCount() > 0) {
                        for (RecordEntry entry : result.getRecords()) {
                            entry.setShardId(shardId);
                        }
                    }
                    if (GET_QPS_METER != null) {
                        GET_QPS_METER.mark(1);
                    }

                    if (GET_RPS_METER != null) {
                        GET_RPS_METER.mark(result.getRecordCount());
                    }
                    return result;
                } finally {
                    if (context != null) {
                        context.stop();
                    }
                }
            }
        });
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

        final String path = String.format(TOPIC_URL, projectName, topicName);
        final AppendFieldRequest request = new AppendFieldRequest().setFieldName(field.getName()).setFieldType(field.getType());

        return callWrapper(new Callable<AppendFieldResult>() {
            @Override
            public AppendFieldResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), AppendFieldResult.class);
            }
        });
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

        final String path = String.format(SHARD_URL, projectName, topicName, shardId);
        final GetMeterInfoRequest request = new GetMeterInfoRequest();

        return callWrapper(new Callable<GetMeterInfoResult>() {
            @Override
            public GetMeterInfoResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), GetMeterInfoResult.class);
            }
        });
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

        final String path = String.format(CONNECTOR_URL, projectName, topicName, connectorType.name().toLowerCase());
        final CreateConnectorRequest request = new CreateConnectorRequest()
                .setSinkStartTime(sinkStartTime)
                .setColumnFields(columnFields)
                .setType(connectorType)
                .setConfig(config);
        return callWrapper(new Callable<CreateConnectorResult>() {
            @Override
            public CreateConnectorResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), CreateConnectorResult.class);
            }
        });
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

        final String path = String.format(CONNECTOR_URL, projectName, topicName, connectorType.name().toLowerCase());
        return callWrapper(new Callable<GetConnectorResult>() {
            @Override
            public GetConnectorResult call() throws Exception {
                return createRequest().path(path).get(GetConnectorResult.class);
            }
        });
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

        final String path = String.format(CONNECTOR_URL, projectName, topicName, connectorType.name().toLowerCase());
        final UpdateConnectorRequest request = new UpdateConnectorRequest().setConfig(config);
        return callWrapper(new Callable<UpdateConnectorResult>() {
            @Override
            public UpdateConnectorResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), UpdateConnectorResult.class);
            }
        });
    }

    @Override
    public ListConnectorResult listConnector(String projectName, String topicName) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        final String path = String.format(CONNECTORS_URL, projectName, topicName);
        return callWrapper(new Callable<ListConnectorResult>() {
            @Override
            public ListConnectorResult call() throws Exception {
                return createRequest().path(path).get(ListConnectorResult.class);
            }
        });
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

        final String path = String.format(CONNECTOR_URL, projectName, topicName, connectorType.name().toLowerCase());
        return callWrapper(new Callable<DeleteConnectorResult>() {
            @Override
            public DeleteConnectorResult call() throws Exception {
                return createRequest().path(path).delete(DeleteConnectorResult.class);
            }
        });
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

        final String path = String.format(CONNECTOR_URL, projectName, topicName, connectorType.name().toLowerCase());
        return callWrapper(new Callable<GetConnectorDoneTimeResult>() {
            @Override
            public GetConnectorDoneTimeResult call() throws Exception {
                return createRequest().path(path)
                        .queryParam("donetime", "")
                        .get(GetConnectorDoneTimeResult.class);
            }
        });
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

        final String path = String.format(CONNECTOR_URL, projectName, topicName, connectorType.name().toLowerCase());
        final ReloadConnectorRequest request = new ReloadConnectorRequest().setShardId(shardId);
        return callWrapper(new Callable<ReloadConnectorResult>() {
            @Override
            public ReloadConnectorResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), ReloadConnectorResult.class);
            }
        });
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

        final String path = String.format(CONNECTOR_URL, projectName, topicName, connectorType.name().toLowerCase());
        final UpdateConnectorStateRequest request = new UpdateConnectorStateRequest().setState(connectorState);
        return callWrapper(new Callable<UpdateConnectorStateResult>() {
            @Override
            public UpdateConnectorStateResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), UpdateConnectorStateResult.class);
            }
        });
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

        final String path = String.format(CONNECTOR_URL, projectName, topicName, connectorType.name().toLowerCase());
        final UpdateConnectorOffsetRequest request = new UpdateConnectorOffsetRequest().setShardId(shardId)
                .setTimestamp(offset.getTimestamp()).setSequence(offset.getSequence());
        return callWrapper(new Callable<UpdateConnectorOffsetResult>() {
            @Override
            public UpdateConnectorOffsetResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), UpdateConnectorOffsetResult.class);
            }
        });
    }

    public GetConnectorShardStatusResult getConnectorShardStatusNotForUser(String projectName, String topicName, ConnectorType connectorType) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (connectorType == null) {
            throw new InvalidParameterException("ConnectorType is null");
        }

        final String path = String.format(CONNECTOR_URL, projectName, topicName, connectorType.name().toLowerCase());
        final GetConnectorShardStatusRequest request = new GetConnectorShardStatusRequest();

        return callWrapper(new Callable<GetConnectorShardStatusResult>() {
            @Override
            public GetConnectorShardStatusResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), GetConnectorShardStatusResult.class);
            }
        });
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

        final String path = String.format(CONNECTOR_URL, projectName, topicName, connectorType.name().toLowerCase());
        final GetConnectorShardStatusRequest request = new GetConnectorShardStatusRequest().setShardId(shardId);

        return callWrapper(new Callable<ConnectorShardStatusEntry>() {
            @Override
            public ConnectorShardStatusEntry call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), ConnectorShardStatusEntry.class);
            }
        });
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

        final String path = String.format(CONNECTOR_URL, projectName, topicName, connectorType.name().toLowerCase());
        final AppendConnectorFieldRequest request = new AppendConnectorFieldRequest().setFieldName(fieldName.toLowerCase());

        return callWrapper(new Callable<AppendConnectorFieldResult>() {
            @Override
            public AppendConnectorFieldResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), AppendConnectorFieldResult.class);
            }
        });
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

        final String path = String.format(SUBSCRIPTIONS_URL, projectName, topicName);
        final CreateSubscriptionRequest request = new CreateSubscriptionRequest().setComment(comment);
        return callWrapper(new Callable<CreateSubscriptionResult>() {
            @Override
            public CreateSubscriptionResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), CreateSubscriptionResult.class);
            }
        });
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

        final String path = String.format(SUBSCRIPTION_URL, projectName, topicName, subId);
        return callWrapper(new Callable<GetSubscriptionResult>() {
            @Override
            public GetSubscriptionResult call() throws Exception {
                return createRequest().path(path).get(GetSubscriptionResult.class);
            }
        });
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

        final String path = String.format(SUBSCRIPTION_URL, projectName, topicName, subId);
        return callWrapper(new Callable<DeleteSubscriptionResult>() {
            @Override
            public DeleteSubscriptionResult call() throws Exception {
                return createRequest().path(path).delete(DeleteSubscriptionResult.class);
            }
        });
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

        final String path = String.format(SUBSCRIPTIONS_URL, projectName, topicName);
        final ListSubscriptionRequest request = new ListSubscriptionRequest()
                .setPageNum(pageNum)
                .setPageSize(pageSize);

        return callWrapper(new Callable<ListSubscriptionResult>() {
            @Override
            public ListSubscriptionResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), ListSubscriptionResult.class);
            }
        });
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

        final String path = String.format(SUBSCRIPTION_URL, projectName, topicName, subId);
        final UpdateSubscriptionRequest request = new UpdateSubscriptionRequest()
                .setComment(comment);

        return callWrapper(new Callable<UpdateSubscriptionResult>() {
            @Override
            public UpdateSubscriptionResult call() throws Exception {
                return createRequest().path(path).put(Entity.json(request), UpdateSubscriptionResult.class);
            }
        });
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

        final String path = String.format(SUBSCRIPTION_URL, projectName, topicName, subId);
        final UpdateSubscriptionRequest request = new UpdateSubscriptionRequest()
                .setState(state);

        return callWrapper(new Callable<UpdateSubscriptionStateResult>() {
            @Override
            public UpdateSubscriptionStateResult call() throws Exception {
                return createRequest().path(path).put(Entity.json(request), UpdateSubscriptionStateResult.class);
            }
        });
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

        final String path = String.format(SUBSCRIPTION_OFFSET_URL, projectName, topicName, subId);
        final OpenSubscriptionSessionRequest request = new OpenSubscriptionSessionRequest().setShardIds(shardIds);
        return callWrapper(new Callable<OpenSubscriptionSessionResult>() {
            @Override
            public OpenSubscriptionSessionResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), OpenSubscriptionSessionResult.class);
            }
        });
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

        final String path = String.format(SUBSCRIPTION_OFFSET_URL, projectName, topicName, subId);
        final GetSubscriptionOffsetRequest request = new GetSubscriptionOffsetRequest().setShardIds(shardIds);
        return callWrapper(new Callable<GetSubscriptionOffsetResult>() {
            @Override
            public GetSubscriptionOffsetResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), GetSubscriptionOffsetResult.class);
            }
        });
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

        final String path = String.format(SUBSCRIPTION_OFFSET_URL, projectName, topicName, subId);
        final CommitSubscriptionOffsetRequest request = new CommitSubscriptionOffsetRequest().setOffsets(offsets);
        return callWrapper(new Callable<CommitSubscriptionOffsetResult>() {
            @Override
            public CommitSubscriptionOffsetResult call() throws Exception {
                return createRequest().path(path).put(Entity.json(request), CommitSubscriptionOffsetResult.class);
            }
        });
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

        final String path = String.format(SUBSCRIPTION_OFFSET_URL, projectName, topicName, subId);
        final ResetSubscriptionOffsetRequest request = new ResetSubscriptionOffsetRequest().setOffsets(offsets);
        return callWrapper(new Callable<ResetSubscriptionOffsetResult>() {
            @Override
            public ResetSubscriptionOffsetResult call() throws Exception {
                return createRequest().path(path).put(Entity.json(request), ResetSubscriptionOffsetResult.class);
            }
        });
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

        final String path = String.format(SUBSCRIPTION_URL, projectName, topicName, consumerGroup);
        final HeartbeatRequest request = new HeartbeatRequest().setConsumerId(consumerId).setVersionId(versionId)
                .setHoldShardList(holdShardList).setReadEndShardList(readEndShardList);
        return callWrapper(new Callable<HeartbeatResult>() {
            @Override
            public HeartbeatResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), HeartbeatResult.class);
            }
        });
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

        final String path = String.format(SUBSCRIPTION_URL, projectName, topicName, consumerGroup);
        final JoinGroupRequest request = new JoinGroupRequest().setSessionTimeout(sessionTimeout);
        return callWrapper(new Callable<JoinGroupResult>() {
            @Override
            public JoinGroupResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), JoinGroupResult.class);
            }
        });
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
        final String path = String.format(SUBSCRIPTION_URL, projectName, topicName, consumerGroup);
        final SyncGroupRequest request = new SyncGroupRequest().setConsumerId(consumerId).setVersionId(versionId)
                .setReleaseShardList(releaseShardList).setReadEndShardList(readEndShardList);
        return callWrapper(new Callable<SyncGroupResult>() {
            @Override
            public SyncGroupResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), SyncGroupResult.class);
            }
        });
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
        final String path = String.format(SUBSCRIPTION_URL, projectName, topicName, consumerGroup);
        final LeaveGroupRequest request = new LeaveGroupRequest().setConsumerId(consumerId).setVersionId(versionId);
        return callWrapper(new Callable<LeaveGroupResult>() {
            @Override
            public LeaveGroupResult call() throws Exception {
                return createRequest().path(path).post(Entity.json(request), LeaveGroupResult.class);
            }
        });
    }
}
