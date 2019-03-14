package com.aliyun.datahub.client.model;

import com.aliyun.datahub.client.impl.serializer.StringToLongSerializer;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

public class SubscriptionOffset {
    @JsonProperty("Timestamp")
    private long timestamp;
    @JsonProperty("Sequence")
    private long sequence;

    @JsonProperty("Version")
    private long versionId;

    @JsonProperty("SessionId")
    @JsonSerialize(using = StringToLongSerializer.class)
    private String sessionId;

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

    public long getVersionId() {
        return versionId;
    }

    public void setVersionId(long versionId) {
        this.versionId = versionId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
}
