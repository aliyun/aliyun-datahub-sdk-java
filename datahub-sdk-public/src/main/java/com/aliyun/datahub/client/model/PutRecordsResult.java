package com.aliyun.datahub.client.model;

import com.aliyun.datahub.client.http.provider.protobuf.BaseProtobufModel;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class PutRecordsResult extends BaseProtobufModel {
    @JsonProperty("FailedRecordCount")
    private int failedRecordCount;

    @JsonProperty("FailedRecords")
    private List<PutErrorEntry> putErrorEntries;

    private List<RecordEntry> failedRecords;

    public int getFailedRecordCount() {
        return failedRecordCount;
    }

    public void setFailedRecordCount(int failedRecordCount) {
        this.failedRecordCount = failedRecordCount;
    }

    public List<PutErrorEntry> getPutErrorEntries() {
        return putErrorEntries;
    }

    public void setPutErrorEntries(List<PutErrorEntry> putErrorEntries) {
        this.putErrorEntries = putErrorEntries;
    }

    public List<RecordEntry> getFailedRecords() {
        return failedRecords;
    }

    public void setFailedRecords(List<RecordEntry> failedRecords) {
        this.failedRecords = failedRecords;
    }
}
