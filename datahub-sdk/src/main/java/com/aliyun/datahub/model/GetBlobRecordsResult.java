package com.aliyun.datahub.model;

import java.util.List;

/**
 * 
 * Represents the output for <a>GetBlobRecords</a>.
 * 
 */
public class GetBlobRecordsResult {

    /**
     * 
     * The next position in the shard from which to start sequentially reading
     * data records.
     * 
     */
    private String nextCursor;
    /**
     * The start sequence of the first record.
     * If no records in result object, then startSeq is -1.
     */
    private long startSeq;
    /**
     * 
     * The data records retrieved from the shard.
     * 
     */
    private List<BlobRecordEntry> records;

    public List<BlobRecordEntry> getRecords() {
        return records;
    }

    public void setRecords(List<BlobRecordEntry> records) {
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
