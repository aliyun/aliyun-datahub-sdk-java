package com.aliyun.datahub.client.impl.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GetRecordsRequest extends BaseRequest {
    @JsonProperty("Cursor")
    private String cursor;

    @JsonProperty("Limit")
    private int limit;

    public GetRecordsRequest() {
        setAction("sub");
    }

    public String getCursor() {
        return cursor;
    }

    public GetRecordsRequest setCursor(String cursor) {
        this.cursor = cursor;
        return this;
    }

    public int getLimit() {
        return limit;
    }

    public GetRecordsRequest setLimit(int limit) {
        this.limit = limit;
        return this;
    }
}
