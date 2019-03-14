package com.aliyun.datahub.client.impl.request;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class HeartbeatRequest extends BaseRequest {
    @JsonProperty("ConsumerId")
    private String consumerId;

    @JsonProperty("VersionId")
    private long versionId;

    @JsonProperty("HoldShardList")
    private List<String> holdShardList;
    
    @JsonProperty("ReadEndShardList")
    private List<String> readEndShardList;

    public HeartbeatRequest() {
        setAction("heartbeat");
    }

    public String getConsumerId() {
        return consumerId;
    }

    public HeartbeatRequest setConsumerId(String consumerId) {
        this.consumerId = consumerId;
        return this;
    }

    public long getVersionId() {
        return versionId;
    }

    public HeartbeatRequest setVersionId(long versionId) {
        this.versionId = versionId;
        return this;
    }

    public List<String> getHoldShardList() {
        return holdShardList;
    }

    public HeartbeatRequest setHoldShardList(List<String> holdShardList) {
        this.holdShardList = holdShardList;
        return this;
    }

    public List<String> getReadEndShardList() {
        return readEndShardList;
    }

    public HeartbeatRequest setReadEndShardList(List<String> readEndShardList) {
        this.readEndShardList = readEndShardList;
        return this;
    }
}
