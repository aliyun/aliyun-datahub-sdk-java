package com.aliyun.datahub.client.http;

import com.aliyun.datahub.client.common.DatahubConstant;
import org.apache.http.conn.ConnectTimeoutException;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpRequest {
    private HttpInterceptor interceptor;
    private String endpoint;
    private String path;
    private HttpMethod method;
    private Map<String, String> headers = new HashMap<>();
    private Map<String, List<Object>> queryParams = new HashMap<>();

    private Client client;
    private Entity<?> entity;
    private int maxRetryCount = 1;

    public HttpRequest(Client client) {
        this.client = client;
    }

    public Client getClient() { return this.client; }

    public HttpRequest interceptor(HttpInterceptor interceptor) {
        this.interceptor = interceptor;
        return this;
    }

    public HttpRequest endpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public HttpRequest path(String path) {
        this.path = path;
        return this;
    }

    public String getPath() {
        return path;
    }

    public HttpRequest method(HttpMethod method) {
        this.method = method;
        return this;
    }

    public HttpMethod getMethod() {
        return method;
    }

    public HttpRequest header(String key, String value) {
        headers.put(key, value);
        return this;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public HttpRequest queryParam(String key, Object value) {
        if (queryParams.containsKey(key)) {
            List<Object> values = queryParams.get(key);
            values.add(value);
        } else {
            List<Object> values = new ArrayList<Object>();
            values.add(value);
            queryParams.put(key, values);
        }
        return this;
    }

    public Map<String, List<Object>> getQueryParams() {
        return queryParams;
    }

    public Entity<?> getEntity() {
        return entity;
    }

    public HttpRequest entity(Entity<?> entity) {
        this.entity = entity;
        return this;
    }

    public HttpRequest maxRetryCount(int retryCount) {
        this.maxRetryCount = retryCount;
        return this;
    }

    public <T> T get(Class<T> returnType) {
        method(HttpMethod.GET);
        return fetchResponse().getEntity(returnType);
    }

    public <T> T post(Class<T> returnType) {
        method(HttpMethod.POST);
        return fetchResponse().getEntity(returnType);
    }

    public <T> T post(Entity<?> entity, Class<T> returnType) {
        entity(entity);
        return post(returnType);
    }

    public <T> T put(Class<T> returnType) {
        method(HttpMethod.PUT);
        return fetchResponse().getEntity(returnType);
    }

    public <T> T put(Entity<?> entity, Class<T> returnType) {
        entity(entity);
        return put(returnType);
    }

    public <T> T delete(Class<T> returnType) {
        method(HttpMethod.DELETE);
        return fetchResponse().getEntity(returnType);
    }

    public HttpResponse fetchResponse() {
        if (interceptor != null) {
            interceptor.preHandle(this);
        }
        HttpResponse response = executeWithRetry(maxRetryCount);
        if (interceptor != null) {
            interceptor.postHandle(response);
        }
        return response;
    }

    private HttpResponse executeWithRetry(int retryCount) {
        int count = 1;
        while (true) {
            try {
                return execute();
            } catch (ProcessingException ex) {
                if (count >= retryCount) {
                    throw ex;
                }

                if (!(ex.getCause() instanceof ConnectTimeoutException) &&
                        !(ex.getCause() instanceof SocketTimeoutException)) {
                    throw ex;
                }

                ++count;
            }
        }
    }

    private HttpResponse execute() {
        String realPath = this.path;
        // TODO: path encode
        WebTarget target = client.target(endpoint).path(realPath);
        for (Map.Entry<String, List<Object>> entry : queryParams.entrySet()) {
            for (Object o : entry.getValue()) {
                String realValue = String.valueOf(o);
                // TODO: encode parameter
                target = target.queryParam(entry.getKey(), realValue);
            }
        }

        Invocation.Builder invocation = target.request();
        // used for auth
        invocation.property(DatahubConstant.PROP_INTER_HTTP_REQUEST, this);

        for (Map.Entry<String, String> header : headers.entrySet()) {
            invocation.header(header.getKey(), header.getValue());
        }

        javax.ws.rs.client.Entity<?> reqEntity = null;
        if (this.entity != null) {
            reqEntity = javax.ws.rs.client.Entity.entity(this.entity.getEntity(), this.entity.getContentType());
        }
        if (this.entity == null && method == HttpMethod.PUT) {
            reqEntity = javax.ws.rs.client.Entity.entity("", MediaType.APPLICATION_JSON_TYPE);
        }
        javax.ws.rs.core.Response response  = (reqEntity == null) ?
                invocation.method(method.name()) : invocation.method(method.name(), reqEntity);
        return new HttpResponse(this, response);
    }
}
