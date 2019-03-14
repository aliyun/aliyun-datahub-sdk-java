package com.aliyun.datahub.model;

public class DeleteDataConnectorResult extends Result {
    public DeleteDataConnectorResult() {
    }

    public DeleteDataConnectorResult(com.aliyun.datahub.client.model.DeleteConnectorResult proxyResult) {
        setRequestId(proxyResult.getRequestId());
    }
}
