package com.aliyun.datahub.client.impl.interceptor;

import com.aliyun.datahub.client.common.DatahubConstant;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.http.interceptor.HttpInterceptor;
import com.aliyun.datahub.client.util.JsonUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DatahubResponseInterceptor extends HttpInterceptor {
    private final static Logger LOGGER = LoggerFactory.getLogger(DatahubResponseInterceptor.class);

    @Override
    public Response intercept(Chain chain) throws IOException {
        return execute(chain.request(), chain);
    }

    private Response execute(Request request, Chain chain) throws IOException {
        Response response = chain.proceed(request);
        String requestId = response.header(DatahubConstant.X_DATAHUB_REQUEST_ID);
        if (requestId == null) {
            LOGGER.warn("requestId is null");
        }
        int status = response.code();
        if (status >= 400) {
            DatahubResponseInterceptor.DatahubResponseError error = new DatahubResponseInterceptor.DatahubResponseError();
            String contentType = response.header(HttpHeaders.CONTENT_TYPE.toLowerCase());
            ResponseBody body = response.body();
            if (body != null) {
                if ("application/json".equalsIgnoreCase(contentType)) {
                    String bodyStr = body.string();
                    error = JsonUtils.fromJson(bodyStr, DatahubResponseInterceptor.DatahubResponseError.class);
                } else {
                    error = new DatahubResponseInterceptor.DatahubResponseError();
                    error.setMessage(body.string());
                }
            }
            throw new DatahubClientException(status,
                    requestId,
                    error == null ? "" : error.getCode(),
                    error == null ? "" : error.getMessage());
        }
        return response;
    }

    private static class DatahubResponseError {
        @JsonProperty("RequestId")
        private String requestId;

        @JsonProperty("ErrorCode")
        private String code;

        @JsonProperty("ErrorMessage")
        private String message;

        public String getRequestId() {
            return requestId;
        }

        public void setRequestId(String requestId) {
            this.requestId = requestId;
        }

        public String getCode() {
            return code;
        }

        public void setCode(String code) {
            this.code = code;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }
}
