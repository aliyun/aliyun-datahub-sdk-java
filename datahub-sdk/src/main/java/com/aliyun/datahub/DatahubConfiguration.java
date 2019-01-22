/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.datahub;

import com.aliyun.datahub.auth.Account;
import com.aliyun.datahub.common.transport.ApacheClientTransport;
import com.aliyun.datahub.model.compress.CompressionFormat;
import com.aliyun.datahub.rest.RestClient;

import java.net.URI;
import java.net.URISyntaxException;

public class DatahubConfiguration {

    /**
     * default socket connect timeout time in seconds
     */
    public static int DEFAULT_SOCKET_CONNECT_TIMEOUT = 180; // seconds

    /**
     * default socket timeout time in seconds
     */
    public static int DEFAULT_SOCKET_TIMEOUT = 300;// seconds

    public static int DEFAULT_TOTAL_CONNECTION_COUNT = 1000;

    public static int DEFAULT_CONNECTION_COUNT_PER_ENDPOINT = 5;

    private Account account;
    private String endpoint;
    private String userAgent = "DATAHUB-SDK-JAVA";

    private String proxyHostName = "";
    private int proxyPort = 0;
    private String proxyUsername = "";
    private String proxyPassword = "";

    private int socketConnectTimeout = DEFAULT_SOCKET_CONNECT_TIMEOUT;
    private int socketTimeout = DEFAULT_SOCKET_TIMEOUT;
    private int totalConnections = DEFAULT_TOTAL_CONNECTION_COUNT;
    private int connectionsPerEndpoint = DEFAULT_CONNECTION_COUNT_PER_ENDPOINT;
    private boolean ignoreCerts = true;
    private CompressionFormat compressionFormat = null;

    public DatahubConfiguration(Account account, String endpoint) {
        this.account = account;
        setEndpoint(endpoint);
    }

    public Account getAccount() {
        return account;
    }

    public void setAccount(Account account) {
        this.account = account;
    }

    public void setEndpoint(String endpoint) {
        try {
            URI uri = new URI(endpoint);

            this.endpoint = uri.toString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getEndpoint(String project) {
        return endpoint;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public String getProxyHostName() {
        return proxyHostName;
    }

    public void setProxyHostName(String proxyHostName) {
        this.proxyHostName = proxyHostName;
    }

    public int getProxyPort() {
        return proxyPort;
    }

    public void setProxyPort(int proxyPort) {
        this.proxyPort = proxyPort;
    }

    public String getProxyUsername() {
        return proxyUsername;
    }

    public void setProxyUsername(String proxyUsername) {
        this.proxyUsername = proxyUsername;
    }

    public String getProxyPassword() {
        return proxyPassword;
    }

    public void setProxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword;
    }

    public int getSocketConnectTimeout() {
        return socketConnectTimeout;
    }

    public void setSocketConnectTimeout(int timeout) {
        this.socketConnectTimeout = timeout;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int timeout) {
        this.socketTimeout = timeout;
    }

    public boolean isIgnoreCerts() {
        return ignoreCerts;
    }

    public void setIgnoreCerts(boolean ignoreCerts) {
        this.ignoreCerts = ignoreCerts;
    }

    public void setCompressionFormat(CompressionFormat compressionFormat) {
        this.compressionFormat = compressionFormat;
    }

    public int getTotalConnections() {
        return totalConnections;
    }

    public void setTotalConnections(int totalConnections) {
        this.totalConnections = totalConnections;
    }

    public int getConnectionsPerEndpoint() {
        return connectionsPerEndpoint;
    }

    public void setConnectionsPerEndpoint(int connectionsPerEndpoint) {
        this.connectionsPerEndpoint = connectionsPerEndpoint;
    }

    public CompressionFormat getCompressionFormat() {
        return compressionFormat;
    }

    public RestClient newRestClient() {
        RestClient client = new RestClient(new ApacheClientTransport(this), compressionFormat);
        client.setAccount(account);
        client.setEndpoint(endpoint);
        client.setUserAgent(userAgent);
        client.setReadTimeout(getSocketTimeout());
        client.setConnectTimeout(getSocketConnectTimeout());
        return client;
    }
}
