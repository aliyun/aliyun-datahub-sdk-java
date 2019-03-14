package com.aliyun.datahub.model;

public class AppendDataConnectorFieldResult extends Result {
    public AppendDataConnectorFieldResult() {
    }

    public AppendDataConnectorFieldResult(com.aliyun.datahub.client.model.AppendConnectorFieldResult proxyResult) {
        setRequestId(proxyResult.getRequestId());
    }
}
