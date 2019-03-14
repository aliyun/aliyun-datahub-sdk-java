package com.aliyun.datahub.model;

public class CreateDataConnectorResult extends Result {
    public CreateDataConnectorResult() {
    }

    public CreateDataConnectorResult(com.aliyun.datahub.client.model.CreateConnectorResult proxyResult) {
        setRequestId(proxyResult.getRequestId());
    }
}
