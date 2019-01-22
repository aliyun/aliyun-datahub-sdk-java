package com.aliyun.datahub.model;

import java.util.List;

/**
 * 
 * Represents the output for <a>GetRecords</a>.
 * 
 */
public class GetRecordsResult {

    /**
     * 
     * The next position in the shard from which to start sequentially reading
     * data records.
     * 
     */
    private String nextCursor;
    /**
     * 
     * The data records retrieved from the shard.
     * 
     */
    private List<RecordEntry> records;
    /**
     * The start sequence of the first record.
     * If no records in result object, then startSeq is -1.
     */
    private long startSeq;


    public List<RecordEntry> getRecords() {
        return records;
    }

    public void setRecords(List<RecordEntry> records) {
        this.records = records;
    }

    public String getNextCursor() {
        return nextCursor;
    }

    public void setNextCursor(String nextCursor) {
        this.nextCursor = nextCursor;
    }

    public int getRecordCount() {
        return records == null ? 0 : records.size();
    }

    public long getStartSeq() {
        return startSeq;
    }

    public void setStartSeq(long startSeq) {
        this.startSeq = startSeq;
    }
}
