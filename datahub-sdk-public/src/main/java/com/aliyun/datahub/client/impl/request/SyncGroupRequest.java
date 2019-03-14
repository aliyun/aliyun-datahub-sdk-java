package com.aliyun.datahub.client.impl.request;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class SyncGroupRequest extends BaseRequest {
    @JsonProperty("ConsumerId")
    private String consumerId;

    @JsonProperty("VersionId")
    private long versionId;

    @JsonProperty("ReleaseShardList")
    private List<String> releaseShardList;

    @JsonProperty("ReadEndShardList")
    private List<String> readEndShardList;

    public SyncGroupRequest() {
        setAction("syncGroup");
    }

    public String getConsumerId() {
        return consumerId;
    }

    public SyncGroupRequest setConsumerId(String consumerId) {
        this.consumerId = consumerId;
        return this;
    }

    public long getVersionId() {
        return versionId;
    }

    public SyncGroupRequest setVersionId(long versionId) {
        this.versionId = versionId;
        return this;
    }

    public List<String> getReleaseShardList() {
        return releaseShardList;
    }

    public SyncGroupRequest setReleaseShardList(List<String> releaseShardList) {
        this.releaseShardList = releaseShardList;
        return this;
    }

    public List<String> getReadEndShardList() {
        return readEndShardList;
    }

    public SyncGroupRequest setReadEndShardList(List<String> readEndShardList) {
        this.readEndShardList = readEndShardList;
        return this;
    }
}
