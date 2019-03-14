package com.aliyun.datahub.client.http;

import java.util.Objects;

public class HttpConfig {
    private int readTimeout = 10000;
    private int connTimeout = 10000;
    private int maxConnPerRoute = 10;
    private int maxConnTotal = 10;

    /**
     * this property is not related to Client
     */
    private int maxRetryCount = 1;

    private boolean enablePbCrc = false;
    private boolean debugRequest = false;
    private CompressType compressType;

    /**
     * e.g. http://1.1.1.1:10001
     */
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

    public int getMaxConnPerRoute() {
        return maxConnPerRoute;
    }

    public HttpConfig setMaxConnPerRoute(int maxConnPerRoute) {
        this.maxConnPerRoute = maxConnPerRoute;
        return this;
    }

    public int getMaxConnTotal() {
        return maxConnTotal;
    }

    public HttpConfig setMaxConnTotal(int maxConnTotal) {
        this.maxConnTotal = maxConnTotal;
        return this;
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    public HttpConfig setMaxRetryCount(int maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
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

    public boolean isEnablePbCrc() {
        return enablePbCrc;
    }

    public HttpConfig setEnablePbCrc(boolean enablePbCrc) {
        this.enablePbCrc = enablePbCrc;
        return this;
    }

    public boolean isDebugRequest() {
        return debugRequest;
    }

    public HttpConfig setDebugRequest(boolean debugRequest) {
        this.debugRequest = debugRequest;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HttpConfig that = (HttpConfig) o;
        return readTimeout == that.readTimeout &&
                connTimeout == that.connTimeout &&
                maxConnPerRoute == that.maxConnPerRoute &&
                maxConnTotal == that.maxConnTotal &&
                enablePbCrc == that.enablePbCrc &&
                debugRequest == that.debugRequest &&
                compressType == that.compressType &&
                Objects.equals(proxyUri, that.proxyUri) &&
                Objects.equals(proxyUsername, that.proxyUsername) &&
                Objects.equals(proxyPassword, that.proxyPassword);
    }

    @Override
    public int hashCode() {
        return Objects.hash(readTimeout, connTimeout, maxConnPerRoute, maxConnTotal, enablePbCrc, debugRequest, compressType, proxyUri, proxyUsername, proxyPassword);
    }

    public enum CompressType {
        LZ4("lz4"),
        DEFLATE("deflate");

        CompressType(String value) {
            this.value = value;
        }

        private String value;

        public String getValue() {
            return value;
        }
    }
}
