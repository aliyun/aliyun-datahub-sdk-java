package com.aliyun.datahub.model;

public class DeleteTopicResult extends Result {
    public DeleteTopicResult() {
    }

    public DeleteTopicResult(com.aliyun.datahub.client.model.DeleteTopicResult proxyResult) {
        setRequestId(proxyResult.getRequestId());
    }
}
