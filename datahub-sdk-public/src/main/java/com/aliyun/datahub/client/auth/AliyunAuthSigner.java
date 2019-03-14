package com.aliyun.datahub.client.auth;

import com.aliyun.datahub.client.http.HttpRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AliyunAuthSigner implements AuthSigner {
    private String accessId;
    private String accessKey;

    public AliyunAuthSigner(String accessId, String accessKey) {
        this.accessId = accessId;
        this.accessKey = accessKey;
    }

    @Override
    public String genAuthSignature(HttpRequest request) {
        Authorization.Request authRequest = new Authorization.Request()
                .setAccessId(accessId)
                .setAccessKey(accessKey)
                .setMethod(request.getMethod().name().toUpperCase())
                .setUrlPath(request.getPath())
                .setHeaders(request.getHeaders())
                .setQueryStrings(getNormParams(request.getQueryParams()));
        return Authorization.getAkAuthorization(authRequest);
    }

    private Map<String,String> getNormParams(Map<String, List<Object>> queryParams) {
        Map<String, String> params = new HashMap<String, String>();
        for (Map.Entry<String, List<Object>> entry : queryParams.entrySet()) {
            if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                params.put(entry.getKey(), String.valueOf(entry.getValue().get(0)));
            }
        }
        return params;
    }
}
