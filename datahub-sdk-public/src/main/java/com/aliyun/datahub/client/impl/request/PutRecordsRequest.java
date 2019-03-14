package com.aliyun.datahub.client.impl.request;

import com.aliyun.datahub.client.model.RecordEntry;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class PutRecordsRequest extends BaseRequest {
    @JsonProperty("Records")
    private List<RecordEntry> records;

    public PutRecordsRequest() {
        setAction("pub");
    }

    public List<RecordEntry> getRecords() {
        return records;
    }

    public PutRecordsRequest setRecords(List<RecordEntry> records) {
        this.records = records;
        return this;
    }
}
