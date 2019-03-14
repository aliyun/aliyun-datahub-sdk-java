package com.aliyun.datahub.model;

import java.util.List;

public class QuerySubscriptionResult extends Result {
	long totalCount;
    List<GetSubscriptionResult> subscriptions;
    
    public long getTotalCount() {
    	return totalCount;
    }
    
    public void setTotalCount(long totalCount) {
    	this.totalCount = totalCount;
    }

    public List<GetSubscriptionResult> getSubscriptions() {
        return subscriptions;
    }

    public void setSubscriptions(List<GetSubscriptionResult> subscriptions) {
        this.subscriptions = subscriptions;
    }
}
