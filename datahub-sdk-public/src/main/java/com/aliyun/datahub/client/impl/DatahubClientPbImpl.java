package com.aliyun.datahub.client.impl;

import com.aliyun.datahub.client.auth.Account;
import com.aliyun.datahub.client.common.DatahubConstant;
import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.http.Entity;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.impl.request.protobuf.GetRecordsRequestPB;
import com.aliyun.datahub.client.impl.request.protobuf.PutRecordsRequestPB;
import com.aliyun.datahub.client.model.*;
import com.aliyun.datahub.client.model.protobuf.GetRecordsResultPB;
import com.aliyun.datahub.client.model.protobuf.PutRecordsResultPB;
import com.aliyun.datahub.client.util.FormatUtils;
import com.codahale.metrics.Timer;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static com.aliyun.datahub.client.common.DatahubConstant.SHARDS_URL;
import static com.aliyun.datahub.client.common.DatahubConstant.SHARD_URL;

public class DatahubClientPbImpl extends DatahubClientJsonImpl {
    public DatahubClientPbImpl(String endpoint, Account account, HttpConfig httpConfig) {
        super(endpoint, account, httpConfig);
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
        final PutRecordsRequestPB request = new PutRecordsRequestPB().setRecords(records);

        return callWrapper(new Callable<PutRecordsResultPB>() {
            @Override
            public PutRecordsResultPB call() throws Exception {
                final Timer.Context context = PUT_LATENCY_TIMER == null ? null : PUT_LATENCY_TIMER.time();
                try {
                    PutRecordsResultPB resultPB = createRequest()
                            .path(path)
                            .header(DatahubConstant.X_DATAHUB_REQUEST_ACTION, "pub")
                            .post(Entity.protobuf(request), PutRecordsResultPB.class);
                    if (resultPB.getFailedRecordCount() > 0) {
                        List<RecordEntry> failedRecords = new ArrayList<>();
                        for (PutErrorEntry errorEntry : resultPB.getPutErrorEntries()) {
                            failedRecords.add(request.getRecords().get(errorEntry.getIndex()));
                        }
                        resultPB.setFailedRecords(failedRecords);
                    }
                    if (PUT_QPS_METER != null) {
                        PUT_QPS_METER.mark(1);
                    }
                    if (PUT_RPS_METER != null) {
                        PUT_RPS_METER.mark(records.size() - resultPB.getFailedRecordCount());
                    }
                    return resultPB;
                } finally {
                    if (context != null) {
                        context.stop();
                    }
                }
            }
        });
    }

    @Override
    public PutRecordsByShardResult putRecordsByShard(String projectName, String topicName, String shardId, final List<RecordEntry> records) {
        if (!FormatUtils.checkProjectName(projectName)) {
            throw new InvalidParameterException("ProjectName format is invalid");
        }

        if (!FormatUtils.checkTopicName(topicName)) {
            throw new InvalidParameterException("TopicName format is invalid");
        }

        if (!FormatUtils.checkShardId(shardId)) {
            throw new InvalidParameterException("ShardId format is invalid");
        }

        if (records == null || records.isEmpty()) {
            throw new InvalidParameterException("Records is null or empty");
        }

        final String path = String.format(SHARD_URL, projectName, topicName, shardId);
        final PutRecordsRequestPB request = new PutRecordsRequestPB().setRecords(records);

        return callWrapper(new Callable<PutRecordsByShardResult>() {
            @Override
            public PutRecordsByShardResult call() throws Exception {
                final Timer.Context context = PUT_LATENCY_TIMER == null ? null : PUT_LATENCY_TIMER.time();
                try {
                    PutRecordsByShardResult result = createRequest()
                            .path(path)
                            .header(DatahubConstant.X_DATAHUB_REQUEST_ACTION, "pub")
                            .post(Entity.protobuf(request), PutRecordsByShardResult.class);

                    if (PUT_QPS_METER != null) {
                        PUT_QPS_METER.mark(1);
                    }

                    if (PUT_RPS_METER != null) {
                        PUT_RPS_METER.mark(records.size());
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
    public GetRecordsResult getRecords(String projectName, String topicName, String shardId, String cursor, int limit) {
        return getRecords(projectName, topicName, shardId, null, cursor, limit);
    }

    @Override
    public GetRecordsResult getRecords(String projectName, String topicName, final String shardId, final RecordSchema schema, String cursor, int limit) {
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
        final GetRecordsRequestPB request = new GetRecordsRequestPB().setCursor(cursor).setLimit(limit);
        return callWrapper(new Callable<GetRecordsResultPB>() {
            @Override
            public GetRecordsResultPB call() throws Exception {
                final Timer.Context context = GET_LATENCY_TIMER == null ? null : GET_LATENCY_TIMER.time();
                try {
                    GetRecordsResultPB resultPB = createRequest()
                            .path(path)
                            .header(DatahubConstant.X_DATAHUB_REQUEST_ACTION, "sub")
                            .post(Entity.protobuf(request), GetRecordsResultPB.class);
                    resultPB.internalSetSchema(schema);
                    resultPB.internalSetShardId(shardId);
                    if (GET_QPS_METER != null) {
                        GET_QPS_METER.mark(1);
                    }
                    if (GET_RPS_METER != null) {
                        GET_RPS_METER.mark(resultPB.getRecordCount());
                    }
                    return resultPB;
                } finally {
                    if (context != null) {
                        context.stop();
                    }
                }
            }
        });
    }
}
