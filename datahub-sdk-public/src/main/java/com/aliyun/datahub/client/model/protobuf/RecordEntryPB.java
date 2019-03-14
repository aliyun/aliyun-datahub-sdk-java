package com.aliyun.datahub.client.model.protobuf;

import com.aliyun.datahub.client.exception.MalformedRecordException;
import com.aliyun.datahub.client.model.*;
import com.google.protobuf.ByteString;
import org.apache.commons.codec.Charsets;

import java.util.HashMap;
import java.util.Map;

public class RecordEntryPB extends RecordEntry {
    private DatahubProtos.RecordEntry proto;
    private DatahubProtos.RecordEntry.Builder builder = null;
    private RecordSchema schema;
    private String shardId;

    public RecordEntryPB() {
        builder = DatahubProtos.RecordEntry.newBuilder();
    }

    public RecordEntryPB(DatahubProtos.RecordEntry recordEntry, RecordSchema schema, String shardId) {
        this.proto = recordEntry;
        this.schema = schema;
        this.shardId = shardId;
    }

    @Override
    public String getShardId() {
        return this.proto.hasShardId() ? this.proto.getShardId() : shardId;
    }

    @Override
    public void setShardId(String shardId) {
        if (shardId != null) {
            this.builder.setShardId(shardId);
        }
    }

    @Override
    public long getSystemTime() {
        return this.proto.getSystemTime();
    }

    @Override
    public void setSystemTime(long systemTime) {
        this.builder.setSystemTime(systemTime);
    }

    @Override
    public long getSequence() {
        return this.proto.getSequence();
    }

    @Override
    public void setSequence(long sequence) {
        this.builder.setSequence(sequence);
    }

    @Override
    public String getCursor() {
        return this.proto.getCursor();
    }

    @Override
    public void setCursor(String cursor) {
        if (cursor != null) {
            this.builder.setCursor(cursor);
        }
    }

    @Override
    public String getNextCursor() {
        return this.proto.getNextCursor();
    }

    @Override
    public void setNextCursor(String nextCursor) {
        if (nextCursor != null) {
            this.builder.setNextCursor(nextCursor);
        }
    }

    @Override
    public String getPartitionKey() {
        return this.proto.getPartitionKey();
    }

    @Override
    public void setPartitionKey(String partitionKey) {
        if (partitionKey != null) {
            this.builder.setPartitionKey(partitionKey);
        }
    }

    @Override
    public String getHashKey() {
        return this.proto.getHashKey();
    }

    @Override
    public void setHashKey(String hashKey) {
        if (hashKey != null) {
            this.builder.setHashKey(hashKey);
        }
    }

    @Override
    public void setAttributes(Map<String, String> attributes) {
        if (attributes != null) {
            DatahubProtos.RecordAttributes.Builder attrsBuilder = DatahubProtos.RecordAttributes.newBuilder();
            for (Map.Entry<String, String> stringPair : attributes.entrySet()) {
                DatahubProtos.StringPair.Builder stringPairBuilder = DatahubProtos.StringPair.newBuilder();
                stringPairBuilder.setKey(stringPair.getKey());
                stringPairBuilder.setValue(stringPair.getValue());
                attrsBuilder.addAttributes(stringPairBuilder.build());
            }
            this.builder.setAttributes(attrsBuilder.build());
        }
    }

    @Override
    public Map<String, String> getAttributes() {
        if (!proto.hasAttributes()) {
            return null;
        }

        Map<String, String> attributes = new HashMap<>();
        for (DatahubProtos.StringPair stringPair : proto.getAttributes().getAttributesList()) {
            attributes.put(stringPair.getKey(), stringPair.getValue());
        }
        return attributes;
    }

    @Override
    public RecordData getRecordData() {
        if (this.schema == null) {
            if (proto.getData().getDataList().size() != 1) {
                throw new MalformedRecordException("Blob record field data size is error");
            }
            DatahubProtos.FieldData fieldData = proto.getData().getData(0);
            return new BlobRecordData(fieldData.getValue().toByteArray());
        }

        TupleRecordData tupleRecordData = new TupleRecordData(proto.getData().getDataList().size());
        for (DatahubProtos.FieldData fieldData : proto.getData().getDataList()) {
            tupleRecordData.internalAddValue(fieldData.hasValue() ? new String(fieldData.getValue().toByteArray(), Charsets.UTF_8) : null);
        }
        tupleRecordData.internalConvertAuxValues(schema);
        return tupleRecordData;
    }

    @Override
    public void setRecordData(RecordData recordData) {
        if (recordData != null) {
            DatahubProtos.RecordData.Builder dataBuilder = DatahubProtos.RecordData.newBuilder();

            if (recordData instanceof TupleRecordData) {
                TupleRecordData data = (TupleRecordData) recordData;
                for (Field field : data.getRecordSchema().getFields()) {
                    DatahubProtos.FieldData.Builder fieldBuilder = DatahubProtos.FieldData.newBuilder();
                    Object value = data.getField(field.getName());
                    if (value != null) {
                        fieldBuilder.setValue(ByteString.copyFrom(value.toString().getBytes(Charsets.UTF_8)));
                    }
                    dataBuilder.addData(fieldBuilder.build());
                }
            } else {
                BlobRecordData data = (BlobRecordData) recordData;
                DatahubProtos.FieldData.Builder fieldBuilder = DatahubProtos.FieldData.newBuilder();
                fieldBuilder.setValue(ByteString.copyFrom(data.getData()));
                dataBuilder.addData(fieldBuilder.build());
            }

            this.builder.setData(dataBuilder.build());
        }
    }

    public DatahubProtos.RecordEntry getProto() {
        return this.builder.build();
    }
}