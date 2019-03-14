package com.aliyun.datahub.client.impl.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ListSubscriptionRequest extends BaseRequest {
    @JsonProperty("PageIndex")
    private int pageNum;

    @JsonProperty("PageSize")
    private int pageSize;

    public ListSubscriptionRequest() {
        setAction("list");
    }

    public int getPageNum() {
        return pageNum;
    }

    public ListSubscriptionRequest setPageNum(int pageNum) {
        this.pageNum = pageNum;
        return this;
    }

    public int getPageSize() {
        return pageSize;
    }

    public ListSubscriptionRequest setPageSize(int pageSize) {
        this.pageSize = pageSize;
        return this;
    }
}
