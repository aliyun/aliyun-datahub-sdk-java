package com.aliyun.datahub.client.auth;

//import com.aliyun.datahub.client.http.HttpRequest;

import okhttp3.Request;

/**
 * Used to specify user access information, and now only support {@link AliyunAccount}
 */
public interface Account {
    void addAuthHeaders(Request.Builder reqBuilder);
}
