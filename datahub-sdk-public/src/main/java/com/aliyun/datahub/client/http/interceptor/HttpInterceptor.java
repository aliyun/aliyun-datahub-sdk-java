package com.aliyun.datahub.client.http.interceptor;

import okhttp3.Interceptor;
import okhttp3.Response;

import java.io.IOException;

public class HttpInterceptor implements Interceptor {
    @Override
    public Response intercept(Chain chain) throws IOException {
        return chain.proceed(chain.request());
    }
}
