package com.aliyun.datahub.client.auth;

//import com.aliyun.datahub.client.http.HttpRequest;

import okhttp3.Request;

public class AliyunAuthSigner implements AuthSigner {
    private String accessId;
    private String accessKey;

    public AliyunAuthSigner(String accessId, String accessKey) {
        this.accessId = accessId;
        this.accessKey = accessKey;
    }

    @Override
    public String genAuthSignature(Request request) {
        Authorization.Request authRequest = new Authorization.Request()
                .setAccessId(accessId)
                .setAccessKey(accessKey)
                .setMethod(request.method().toUpperCase())
                .setUrlPath(request.url().encodedPath())
                .setHeaders(request.headers().toMultimap())
                .setQueryStrings(request.url().encodedQuery());
        return Authorization.getAkAuthorization(authRequest);
    }
}
