package com.aliyun.datahub.client.model.protobuf;

import com.aliyun.datahub.client.model.PutErrorEntry;

public class PutErrorEntryPB extends PutErrorEntry {
    private DatahubProtos.FailedRecord proto;

    public PutErrorEntryPB(DatahubProtos.FailedRecord proto) {
        this.proto = proto;
    }

    @Override
    public int getIndex() {
        return this.proto.getIndex();
    }

    @Override
    public String getErrorcode() {
        return this.proto.getErrorCode();
    }

    @Override
    public String getMessage() {
        return this.proto.getErrorMessage();
    }
}
