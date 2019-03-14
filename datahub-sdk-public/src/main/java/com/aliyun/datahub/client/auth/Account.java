package com.aliyun.datahub.client.auth;

import com.aliyun.datahub.client.http.HttpRequest;

/**
 * Used to specify user access information, and now only support {@link AliyunAccount}
 */
public interface Account {
    String genAuthSignature(HttpRequest request);

    void addAuthHeaders(HttpRequest request);
}
