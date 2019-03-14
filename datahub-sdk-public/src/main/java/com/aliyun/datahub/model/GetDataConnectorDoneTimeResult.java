package com.aliyun.datahub.model;

import com.aliyun.datahub.client.model.GetConnectorDoneTimeResult;

public class GetDataConnectorDoneTimeResult extends Result {
    private com.aliyun.datahub.client.model.GetConnectorDoneTimeResult proxyResult;

    public GetDataConnectorDoneTimeResult(GetConnectorDoneTimeResult proxyResult) {
        this.proxyResult = proxyResult;
        setRequestId(proxyResult.getRequestId());
    }

    public GetDataConnectorDoneTimeResult(){
        proxyResult = new com.aliyun.datahub.client.model.GetConnectorDoneTimeResult();
    }

    public Long getDoneTime() {
        return proxyResult.getDoneTime();
    }

    public void setDoneTime(Long doneTime) {
        proxyResult.setDoneTime(doneTime);
    }
}
