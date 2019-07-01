package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class GetRecordsResult extends BaseResult {
    /**
     * The cursor used for the next read.
     */
    @JsonProperty("NextCursor")
    private String nextCursor;

    /**
     * Record count returned.
     */
    @JsonProperty("RecordCount")
    private int recordCount;

    /**
     * Indicate the sequence of the first record.
     */
    @JsonProperty("StartSeq")
    private long startSequence;

    /**
     * RecordEntity list. You can get detail information by iterator it.
     */
    @JsonProperty("Records")
    private List<RecordEntry> records;

    public String getNextCursor() {
        return nextCursor;
    }

    public void setNextCursor(String nextCursor) {
        this.nextCursor = nextCursor;
    }

    public int getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(int recordCount) {
        this.recordCount = recordCount;
    }

    public long getStartSequence() {
        return startSequence;
    }

    public void setStartSequence(long startSequence) {
        this.startSequence = startSequence;
    }

    public List<RecordEntry> getRecords() {
        return records;
    }

    public void setRecords(List<RecordEntry> records) {
        this.records = records;
    }
}
