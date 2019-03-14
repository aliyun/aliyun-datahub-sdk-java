package com.aliyun.datahub.model;

import com.aliyun.datahub.client.model.GetMeterInfoResult;

/**
 * 
 * Represents the output for <a>GetMeteringInfo</a>.
 * 
 */
public class GetMeteringInfoResult extends Result {
    com.aliyun.datahub.client.model.GetMeterInfoResult proxyResult;

    public GetMeteringInfoResult(GetMeterInfoResult proxyResult) {
        this.proxyResult = proxyResult;
        setRequestId(proxyResult.getRequestId());
    }

    public GetMeteringInfoResult() {
        proxyResult = new com.aliyun.datahub.client.model.GetMeterInfoResult();
    }

    private long activeTime;
    private long storage;

    public long getStorage() {
        return proxyResult.getStorage();
    }

    public void setStorage(long storage) {
        proxyResult.setStorage(storage);
    }

    public long getActiveTime() {
        return proxyResult.getActiveTime();
    }

    public void setActiveTime(long activeTime) {
        proxyResult.setActiveTime(activeTime);
    }
}
