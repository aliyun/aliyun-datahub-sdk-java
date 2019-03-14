package com.aliyun.datahub.model;

import com.aliyun.datahub.client.model.CommitSubscriptionOffsetResult;

public class CommitOffsetResult extends Result {
    public CommitOffsetResult() {
    }

    public CommitOffsetResult(CommitSubscriptionOffsetResult proxyResult) {
        setRequestId(proxyResult.getRequestId());
    }
}
