package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ListSubscriptionResult extends BaseResult {
    @JsonProperty("TotalCount")
    private long totalCount;

    @JsonProperty("Subscriptions")
    private List<SubscriptionEntry> subscriptions;

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public List<SubscriptionEntry> getSubscriptions() {
        return subscriptions;
    }

    public void setSubscriptions(List<SubscriptionEntry> subscriptions) {
        this.subscriptions = subscriptions;
    }
}
