package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GetCursorResult extends BaseResult {
    /**
     * Cursor of the data. Then can read data with this cursor.
     */
    @JsonProperty("Cursor")
    private String cursor;

    /**
     * Record time of the data. This is the data writing time.
     */
    @JsonProperty("RecordTime")
    private long  timestamp;

    /**
     * Sequence of the data. Each data written in datahub has a sequence.
     */
    @JsonProperty("Sequence")
    private long  sequence;

    public String getCursor() {
        return cursor;
    }

    public void setCursor(String cursor) {
        this.cursor = cursor;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }
}
