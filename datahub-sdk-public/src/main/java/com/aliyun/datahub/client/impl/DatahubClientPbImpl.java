package com.aliyun.datahub.client.impl;

import com.aliyun.datahub.client.auth.Account;
import com.aliyun.datahub.client.exception.InvalidParameterException;
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

public class DatahubClientPbImpl extends DatahubClientJsonImpl {
    public DatahubClientPbImpl(String endpoint, Account account, HttpConfig httpConfig, String userAgent) {
        super(endpoint, account, httpConfig, userAgent);
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

        final PutRecordsRequestPB request = new PutRecordsRequestPB().setRecords(records);
        Timer.Context context = PUT_LATENCY_TIMER == null ? null : PUT_LATENCY_TIMER.time();
        try {
            PutRecordsResultPB result = callWrapper(getService().putPbRecords(projectName, topicName, request));
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

        final PutRecordsRequestPB request = new PutRecordsRequestPB().setRecords(records);

        final Timer.Context context = PUT_LATENCY_TIMER == null ? null : PUT_LATENCY_TIMER.time();
        try {
            PutRecordsByShardResult result = callWrapper(getService().putPbRecordsByShard(projectName,
                    topicName, shardId, request));
            if (result != null) {
                if (PUT_QPS_METER != null) {
                    PUT_QPS_METER.mark(1);
                }

                if (PUT_RPS_METER != null) {
                    PUT_RPS_METER.mark(records.size());
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

        final GetRecordsRequestPB request = new GetRecordsRequestPB().setCursor(cursor).setLimit(limit);
        final Timer.Context context = GET_LATENCY_TIMER == null ? null : GET_LATENCY_TIMER.time();
        try {
            GetRecordsResultPB result = callWrapper(getService().getPBRecords(projectName,
                    topicName, shardId, request));
            if (result != null) {
                result.internalSetSchema(schema);
                result.internalSetShardId(shardId);
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
}
