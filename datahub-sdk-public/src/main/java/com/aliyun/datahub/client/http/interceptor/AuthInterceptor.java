package com.aliyun.datahub.client.http.interceptor;

import com.aliyun.datahub.client.auth.Account;
import com.aliyun.datahub.client.common.DatahubConstant;
import com.aliyun.datahub.client.http.HttpRequest;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.ext.WriterInterceptor;
import javax.ws.rs.ext.WriterInterceptorContext;
import java.io.IOException;


public class AuthInterceptor implements WriterInterceptor {
    private Account account;

    public AuthInterceptor(Account account) {
        this.account = account;
    }

    @Override
    public void aroundWriteTo(WriterInterceptorContext writerContext) throws IOException, WebApplicationException {
        writerContext.proceed();
        // !! must flush here
        writerContext.getOutputStream().flush();

        HttpRequest httpRequest = (HttpRequest) writerContext.getProperty(DatahubConstant.PROP_INTER_HTTP_REQUEST);
        if (httpRequest != null && account != null) {
            String rawSize = (String) writerContext.getHeaders().getFirst(DatahubConstant.X_DATAHUB_CONTENT_RAW_SIZE);
            if (rawSize != null) {
                httpRequest.header(DatahubConstant.X_DATAHUB_CONTENT_RAW_SIZE, rawSize);
            }

            String authStr = account.genAuthSignature(httpRequest);
            if (authStr != null) {
                // remove old
                writerContext.getHeaders().remove(HttpHeaders.AUTHORIZATION);
                // put new
                writerContext.getHeaders().putSingle(HttpHeaders.AUTHORIZATION, authStr);
            }
        }
    }
}
