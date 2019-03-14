package com.aliyun.datahub.model;

public class ReloadDataConnectorResult extends Result {
    public ReloadDataConnectorResult() {
    }

    public ReloadDataConnectorResult(com.aliyun.datahub.client.model.ReloadConnectorResult proxyResult) {
        setRequestId(proxyResult.getRequestId());
    }
}
