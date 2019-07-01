package com.aliyun.datahub.client.model.protobuf;

import com.aliyun.datahub.client.http.converter.BaseProtobufModel;
import com.aliyun.datahub.client.model.PutErrorEntry;
import com.aliyun.datahub.client.model.PutRecordsResult;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;

public class PutRecordsResultPB extends PutRecordsResult implements BaseProtobufModel {
    private DatahubProtos.PutRecordsResponse proto;

    @Override
    public int getFailedRecordCount() {
        return this.proto.getFailedCount();
    }

    @Override
    public List<PutErrorEntry> getPutErrorEntries() {
        List<PutErrorEntry> putErrorEntryList = new ArrayList<>();
        for (DatahubProtos.FailedRecord failedEntry : this.proto.getFailedRecordsList()) {
            putErrorEntryList.add(convertFromProtoFormat(failedEntry));
        }
        return putErrorEntryList;
    }

    private PutErrorEntry convertFromProtoFormat(DatahubProtos.FailedRecord recordEntry) {
        return new PutErrorEntryPB(recordEntry);
    }

    @Override
    public Message getMessage() {
        return null;
    }

    @Override
    public void setMessage(Message message) {
        this.proto = (DatahubProtos.PutRecordsResponse) message;
    }

    public static Message.Builder newBuilder() {
        return DatahubProtos.PutRecordsResponse.newBuilder();
    }
}
