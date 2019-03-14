package com.aliyun.datahub.client.model;

public class ConnectorOffset {
    private long timestamp = -1;
    private long sequence = -1;

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
