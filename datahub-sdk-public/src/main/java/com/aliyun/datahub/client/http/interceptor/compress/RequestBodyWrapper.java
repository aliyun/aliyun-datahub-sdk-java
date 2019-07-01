package com.aliyun.datahub.client.http.interceptor.compress;

import okhttp3.RequestBody;

public class RequestBodyWrapper {
    private long rawSize;
    private RequestBody body;

    public RequestBodyWrapper(long rawSize, RequestBody body) {
        this.rawSize = rawSize;
        this.body = body;
    }

    public long getRawSize() {
        return rawSize;
    }

    public RequestBody getBody() {
        return body;
    }
}
