package com.aliyun.datahub.client.auth;

import okhttp3.Request;

public interface AuthSigner {
    String genAuthSignature(Request request);
}
