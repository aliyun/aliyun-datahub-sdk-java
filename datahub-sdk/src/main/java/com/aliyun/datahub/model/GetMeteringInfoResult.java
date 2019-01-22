package com.aliyun.datahub.model;

/**
 * 
 * Represents the output for <a>GetMeteringInfo</a>.
 * 
 */
public class GetMeteringInfoResult {
    private long activeTime;
    private long storage;

    public long getStorage() {
        return storage;
    }

    public void setStorage(long storage) {
        this.storage = storage;
    }

    public long getActiveTime() {
        return activeTime;
    }

    public void setActiveTime(long activeTime) {
        this.activeTime = activeTime;
    }


}
