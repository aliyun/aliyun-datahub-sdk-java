package com.aliyun.datahub.common.transport;

import com.aliyun.datahub.DatahubConstants;
import com.aliyun.datahub.common.transport.Headers;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.common.util.DateUtils;
import com.aliyun.datahub.common.util.RevisionUtils;
import com.aliyun.datahub.exception.InvalidParameterException;
import com.aliyun.datahub.rest.DatahubHttpHeaders;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DefaultRequest {

    protected HttpMethod httpMethod = HttpMethod.GET;

    protected String resource;

    protected final Map<String, String> params = new HashMap<String, String>();

    protected final Map<String, String> headers = new HashMap<String, String>();

    private static final String
            USER_AGENT =
            "JavaSDK" + " Revision:" + RevisionUtils.getGitRevision()
                    + " Version:" + RevisionUtils.getMavenVersion() + " JavaVersion:" + RevisionUtils.getJavaVersion();

    protected byte[] body = null;

    {
        this.headers.put(Headers.CONTENT_TYPE, "application/json");
        this.headers.put(Headers.CONTENT_LENGTH, "0");
        this.headers.put(Headers.DATE, DateUtils.formatRfc822Date(new Date()));
        this.headers.put(Headers.USER_AGENT, USER_AGENT);

        this.headers.put(DatahubHttpHeaders.HEADER_DATAHUB_CLIENT_VERSION, DatahubConstants.VERSION);
    }

    public void addParam(String key, String value) {
        this.params.put(key, value);
    }

    public void addParam(Map<String, String> exParams) {
        this.params.putAll(exParams);
    }

    public void setHttpMethod(HttpMethod httpMethod) {
        this.httpMethod = httpMethod;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public void addHeader(String key, String value) {
        this.headers.put(key, value);
    }

    public void addHeader(Map<String, String> exHeaders) {
        this.headers.putAll(exHeaders);
    }

    public void setBody(String body) {
        assert (StringUtils.isNoneBlank(body));
        try {
            setBody(body.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new InvalidParameterException("unsupported encoding");
        }
    }

    public void setBody(byte[] body) {
        if (body != null) {
            this.body = body;
            //this.headers.put(Headers.CONTENT_MD5, CommonUtils.generatorMD5(this.body));
            this.headers.put(Headers.CONTENT_LENGTH, String.valueOf(this.body.length));
        }
    }

    public HttpMethod getHttpMethod() {
        return httpMethod;
    }

    public String getResource() {
        return resource;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public byte[] getBody() {
        return body;
    }

}
