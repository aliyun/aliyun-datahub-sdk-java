package com.aliyun.datahub.client.impl.request.protobuf;

import com.aliyun.datahub.client.http.converter.BaseProtobufModel;
import com.aliyun.datahub.client.model.BaseResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.protobuf.DatahubProtos;
import com.aliyun.datahub.client.model.protobuf.RecordEntryPB;
import com.google.protobuf.Message;

import java.util.List;

public class PutRecordsRequestPB extends BaseResult implements BaseProtobufModel {
    private DatahubProtos.PutRecordsRequest.Builder builder = DatahubProtos.PutRecordsRequest.newBuilder();
    private List<RecordEntry> originRecords;

    public PutRecordsRequestPB setRecords(List<RecordEntry> records) {
        this.originRecords = records;
        for (RecordEntry entry : records) {
            builder.addRecords(convertToProtoFormat(entry));
        }
        return this;
    }

    public List<RecordEntry> getRecords() {
        return this.originRecords;
    }

    private DatahubProtos.RecordEntry convertToProtoFormat(final RecordEntry entry) {
        RecordEntryPB recordEntryPB = new RecordEntryPB() {{
            setShardId(entry.getShardId());
            setHashKey(entry.getHashKey());
            setPartitionKey(entry.getPartitionKey());
            setAttributes(entry.getAttributes());
            setRecordData(entry.getRecordData());
        }};

        return recordEntryPB.getProto();
    }


    @Override
    public Message getMessage() {
        return builder.build();
    }

    @Override
    public void setMessage(Message message) {

    }
}
