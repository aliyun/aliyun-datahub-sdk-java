package com.aliyun.datahub.client.http;

import java.util.Objects;

public class HttpConfig {
    private int readTimeout = 10000;
    private int connTimeout = 10000;
    private int maxRetryCount = 1;

    private boolean enableH2C = false;
    private boolean debugRequest = false;
    private boolean enablePbCrc = false;
    private CompressType compressType;

    private String proxyUri;
    private String proxyUsername;
    private String proxyPassword;

    public int getReadTimeout() {
        return readTimeout;
    }

    public HttpConfig setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
        return this;
    }

    public int getConnTimeout() {
        return connTimeout;
    }

    public HttpConfig setConnTimeout(int connTimeout) {
        this.connTimeout = connTimeout;
        return this;
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    public HttpConfig setMaxRetryCount(int maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
        return this;
    }

    public boolean isDebugRequest() {
        return debugRequest;
    }

    public HttpConfig setDebugRequest(boolean debugRequest) {
        this.debugRequest = debugRequest;
        return this;
    }

    public boolean isEnableH2C() {
        return enableH2C;
    }

    public HttpConfig setEnableH2C(boolean enableH2C) {
        this.enableH2C = enableH2C;
        return this;
    }

    public boolean isEnablePbCrc() {
        return enablePbCrc;
    }

    public HttpConfig setEnablePbCrc(boolean enablePbCrc) {
        this.enablePbCrc = enablePbCrc;
        return this;
    }

    public CompressType getCompressType() {
        return compressType;
    }

    public HttpConfig setCompressType(CompressType compressType) {
        this.compressType = compressType;
        return this;
    }

    public String getProxyUri() {
        return proxyUri;
    }

    public HttpConfig setProxyUri(String proxyUri) {
        this.proxyUri = proxyUri;
        return this;
    }

    public String getProxyUsername() {
        return proxyUsername;
    }

    public HttpConfig setProxyUsername(String proxyUsername) {
        this.proxyUsername = proxyUsername;
        return this;
    }

    public String getProxyPassword() {
        return proxyPassword;
    }

    public HttpConfig setProxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HttpConfig that = (HttpConfig) o;
        return readTimeout == that.readTimeout &&
                connTimeout == that.connTimeout &&
                enableH2C == that.enableH2C &&
                debugRequest == that.debugRequest &&
                enablePbCrc == that.enablePbCrc &&
                compressType == that.compressType &&
                Objects.equals(proxyUri, that.proxyUri) &&
                Objects.equals(proxyUsername, that.proxyUsername) &&
                Objects.equals(proxyPassword, that.proxyPassword);
    }

    @Override
    public int hashCode() {
        return Objects.hash(readTimeout, connTimeout, enableH2C, debugRequest, enablePbCrc, compressType, proxyUri, proxyUsername, proxyPassword);
    }

    public enum CompressType {
        DEFLATE, LZ4
    }
}
