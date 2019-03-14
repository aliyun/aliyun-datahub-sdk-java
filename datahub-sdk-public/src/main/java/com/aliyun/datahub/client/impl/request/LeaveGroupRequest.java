package com.aliyun.datahub.client.impl.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LeaveGroupRequest extends BaseRequest {
    @JsonProperty("ConsumerId")
    private String consumerId;

    @JsonProperty("VersionId")
    private long versionId;

    public LeaveGroupRequest() {
        setAction("leaveGroup");
    }

    public String getConsumerId() {
        return consumerId;
    }

    public LeaveGroupRequest setConsumerId(String consumerId) {
        this.consumerId = consumerId;
        return this;
    }

    public long getVersionId() {
        return versionId;
    }

    public LeaveGroupRequest setVersionId(long versionId) {
        this.versionId = versionId;
        return this;
    }
}
