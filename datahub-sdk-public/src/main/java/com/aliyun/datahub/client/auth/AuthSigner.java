package com.aliyun.datahub.client.auth;

import com.aliyun.datahub.client.http.HttpRequest;

public interface AuthSigner {
    String genAuthSignature(HttpRequest request);
}
