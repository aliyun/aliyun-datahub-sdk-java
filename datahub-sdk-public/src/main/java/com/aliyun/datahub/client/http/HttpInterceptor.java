package com.aliyun.datahub.client.http;

import com.aliyun.datahub.client.auth.Account;

public class HttpInterceptor {
    public void setUserAgent(String userAgent) {}

    public Account getAccount() {
        return null;
    }

    public void preHandle(HttpRequest request) {
    }

    public void postHandle(HttpResponse response) {
    }
}
