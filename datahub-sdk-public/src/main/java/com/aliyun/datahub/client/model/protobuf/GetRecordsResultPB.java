package com.aliyun.datahub.client.model.protobuf;

import com.aliyun.datahub.client.http.converter.BaseProtobufModel;
import com.aliyun.datahub.client.model.GetRecordsResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;

public class GetRecordsResultPB extends GetRecordsResult implements BaseProtobufModel {
    private DatahubProtos.GetRecordsResponse proto;
    private RecordSchema schema;
    private String shardId;

    @Override
    public Message getMessage() {
        return null;
    }

    @Override
    public void setMessage(Message message) {
        this.proto = (DatahubProtos.GetRecordsResponse) message;
    }

    @Override
    public String getNextCursor() {
        return this.proto.getNextCursor();
    }

    @Override
    public int getRecordCount() {
        return this.proto.getRecordCount();
    }

    @Override
    public long getStartSequence() {
        return this.proto.getStartSequence();
    }

    @Override
    public List<RecordEntry> getRecords() {
        List<RecordEntry> recordEntries = new ArrayList<>();

        for (DatahubProtos.RecordEntry entry : proto.getRecordsList()) {
            recordEntries.add(convertFromProtoFormat(entry));
        }
        return recordEntries;
    }

    public void internalSetSchema(RecordSchema schema) {
        this.schema = schema;
    }

    public void internalSetShardId(String shardId) {
        this.shardId = shardId;
    }

    private RecordEntry convertFromProtoFormat(DatahubProtos.RecordEntry recordEntry) {
        return new RecordEntryPB(recordEntry, schema, shardId);
    }

    public static Message.Builder newBuilder() {
        return DatahubProtos.GetRecordsResponse.newBuilder();
    }
}
