package com.aliyun.datahub.client.impl.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UpdateConnectorOffsetRequest extends BaseRequest {
    @JsonProperty("ShardId")
    private String shardId;

    @JsonProperty("CurrentTime")
    private long timestamp = -1;

    @JsonProperty("CurrentSequence")
    private long sequence = -1;

    public UpdateConnectorOffsetRequest() {
        setAction("updateshardcontext");
    }

    public String getShardId() {
        return shardId;
    }

    public UpdateConnectorOffsetRequest setShardId(String shardId) {
        this.shardId = shardId;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public UpdateConnectorOffsetRequest setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public long getSequence() {
        return sequence;
    }

    public UpdateConnectorOffsetRequest setSequence(long sequence) {
        this.sequence = sequence;
        return this;
    }
}
