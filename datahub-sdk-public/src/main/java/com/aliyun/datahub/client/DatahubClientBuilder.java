package com.aliyun.datahub.client;

import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.impl.DatahubClientJsonImpl;
import com.aliyun.datahub.client.impl.DatahubClientPbImpl;

public class DatahubClientBuilder {
    private String userAgent;
    private DatahubConfig datahubConfig;
    private HttpConfig httpConfig;

    private DatahubClientBuilder() {
    }

    public static DatahubClientBuilder newBuilder() {
        return new DatahubClientBuilder();
    }

    public DatahubConfig getDatahubConfig() {
        return datahubConfig;
    }

    public DatahubClientBuilder setDatahubConfig(DatahubConfig datahubConfig) {
        this.datahubConfig = datahubConfig;
        return this;
    }

    public HttpConfig getHttpConfig() {
        return httpConfig;
    }

    public DatahubClientBuilder setHttpConfig(HttpConfig httpConfig) {
        this.httpConfig = httpConfig;
        return this;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public DatahubClientBuilder setUserAgent(String userAgent) {
        this.userAgent = userAgent;
        return this;
    }

    public DatahubClient build() {
        if (datahubConfig == null) {
            throw new InvalidParameterException("DatahubConfig is not set");
        }
        if (httpConfig == null) {
            httpConfig = new HttpConfig();
        }

        if (datahubConfig.isEnableBinary()) {
            return new DatahubClientPbImpl(datahubConfig.getEndpoint(),
                    datahubConfig.getAccount(), httpConfig, userAgent);
        }

        return new DatahubClientJsonImpl(datahubConfig.getEndpoint(),
                datahubConfig.getAccount(), httpConfig, userAgent);
    }

}
