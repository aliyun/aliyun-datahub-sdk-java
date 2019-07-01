package com.aliyun.datahub.client.http.interceptor;

public class InterceptorWrapper {
    private AuthInterceptor auth = new AuthInterceptor(null);
    private HttpInterceptor response = new HttpInterceptor();

    public AuthInterceptor getAuth() {
        return auth;
    }

    public InterceptorWrapper setAuth(AuthInterceptor auth) {
        this.auth = auth;
        return this;
    }

    public HttpInterceptor getResponse() {
        return response;
    }

    public InterceptorWrapper setResponse(HttpInterceptor response) {
        this.response = response;
        return this;
    }
}
