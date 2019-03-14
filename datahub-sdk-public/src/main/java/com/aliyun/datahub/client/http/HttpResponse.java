package com.aliyun.datahub.client.http;

import com.aliyun.datahub.client.common.DatahubConstant;
import com.aliyun.datahub.client.model.BaseResult;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

public class HttpResponse {
    private int httpStatus;
    private Map<String, String> headers = new HashMap<>();
    private Response response;

    public HttpResponse(HttpRequest request, Response response) {
        this.response = response;
        this.httpStatus = response.getStatus();

        for (String key : response.getHeaders().keySet()) {
            headers.put(key.toLowerCase(), response.getHeaderString(key));
        }
    }

    public <T> T getEntity(Class<T> returnType) {
        try {
            if (returnType != null) {
                T result = getEntityInter(returnType);
                if (result == null) {
                    try {
                        result = returnType.newInstance();
                    } catch (InstantiationException | IllegalAccessException e) {
                        //
                    }
                }

                if (result != null
                        && BaseResult.class.isAssignableFrom(returnType)) {
                    String requestId = headers.get(DatahubConstant.X_DATAHUB_REQUEST_ID);
                    ((BaseResult)result).setRequestId(requestId);
                }
                return result;
            }
            return null;
        } finally {
            response.close();
        }
    }

    private <T> T getEntityInter(Class<T> returnType) {
        if (isMediaSupport()) {
            return response.readEntity(returnType);
        } else {
            String content = response.readEntity(String.class);
            if (String.class.equals(returnType)) {
                return returnType.cast(content);
            } else {
                return null;
            }
        }
    }

    private boolean isMediaSupport() {
        String mediaType = headers.get(HttpHeaders.CONTENT_TYPE.toLowerCase());
        return mediaType != null && (
                mediaType.contains("application/json") || mediaType.contains("application/x-protobuf"));
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public void close() {
        this.response.close();
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public String getHeader(String key) {
        return headers.get(key);
    }
}
