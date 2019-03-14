package com.aliyun.datahub.model;

/**
 * 
 * Represents the output for <a>GetCursor</a>.
 * 
 */
public class GetCursorResult extends Result {
    private com.aliyun.datahub.client.model.GetCursorResult proxyResult;

    public GetCursorResult(com.aliyun.datahub.client.model.GetCursorResult proxyResult) {
        this.proxyResult = proxyResult;
        setRequestId(proxyResult.getRequestId());
    }

    public GetCursorResult() {
        proxyResult = new com.aliyun.datahub.client.model.GetCursorResult();
    }

    public String getCursor() {
        return proxyResult.getCursor();
    }

    public void setCursor(String cursor) {
        proxyResult.setCursor(cursor);
    }

    public long getRecordTime() {
        return proxyResult.getTimestamp();
    }

    public void setRecordTime(long recordTime) {
        proxyResult.setTimestamp(recordTime);
    }

    public long getSequence() {
        return proxyResult.getSequence();
    }

    public void setSequence(long sequence) {
        proxyResult.setSequence(sequence);
    }
}
