package com.aliyun.datahub.model;

/**
 * 
 * Represents the output for <a>GetCursor</a>.
 * 
 */
public class GetCursorResult {
    private String cursor;
    private long  recordTime;
    private long  sequence;

    public String getCursor() {
        return cursor;
    }

    public void setCursor(String cursor) {
        this.cursor = cursor;
    }

    public long getRecordTime() {
        return recordTime;
    }

    public void setRecordTime(long recordTime) {
        this.recordTime = recordTime;
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }
}
