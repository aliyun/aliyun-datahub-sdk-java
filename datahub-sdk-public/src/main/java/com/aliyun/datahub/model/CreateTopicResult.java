package com.aliyun.datahub.model;

public class CreateTopicResult extends Result {
    public CreateTopicResult() {
    }

    public CreateTopicResult(com.aliyun.datahub.client.model.CreateTopicResult proxyResult) {
        setRequestId(proxyResult.getRequestId());
    }
}
