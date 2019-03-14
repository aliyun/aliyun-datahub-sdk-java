package com.aliyun.datahub.client.impl.request;

import com.aliyun.datahub.client.impl.serializer.UpdateSubscriptionRequestSerializer;
import com.aliyun.datahub.client.model.SubscriptionState;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = UpdateSubscriptionRequestSerializer.class)
public class UpdateSubscriptionRequest {
    // 目前接口更新不能同时更新state和comment, 只有当state为-1时才会更新comment
    private SubscriptionState state;

    private String comment;

    public SubscriptionState getState() {
        return state;
    }

    public UpdateSubscriptionRequest setState(SubscriptionState state) {
        this.state = state;
        return this;
    }

    public String getComment() {
        return comment;
    }

    public UpdateSubscriptionRequest setComment(String comment) {
        this.comment = comment;
        return this;
    }
}
