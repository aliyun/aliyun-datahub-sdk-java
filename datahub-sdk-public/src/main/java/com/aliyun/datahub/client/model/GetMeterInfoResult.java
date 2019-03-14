package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GetMeterInfoResult extends BaseResult {
    /**
     * The alive time that a shard lasts.
     */
    @JsonProperty("ActiveTime")
    private long activeTime;

    /**
     * The data storage size of a shard.
     */
    @JsonProperty("Storage")
    private long storage;

    public long getActiveTime() {
        return activeTime;
    }

    public void setActiveTime(long activeTime) {
        this.activeTime = activeTime;
    }

    public long getStorage() {
        return storage;
    }

    public void setStorage(long storage) {
        this.storage = storage;
    }
}
