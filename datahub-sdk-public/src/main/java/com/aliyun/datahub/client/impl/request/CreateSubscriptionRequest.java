package com.aliyun.datahub.client.impl.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CreateSubscriptionRequest extends BaseRequest {
    @JsonProperty("Comment")
    private String comment;

    public CreateSubscriptionRequest() {
        setAction("create");
    }

    public String getComment() {
        return comment;
    }

    public CreateSubscriptionRequest setComment(String comment) {
        this.comment = comment;
        return this;
    }
}
