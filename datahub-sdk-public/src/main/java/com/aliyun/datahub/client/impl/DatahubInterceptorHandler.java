package com.aliyun.datahub.client.impl;

import com.aliyun.datahub.client.auth.Account;
import com.aliyun.datahub.client.common.DatahubConstant;
import com.aliyun.datahub.client.common.DateFormat;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.http.HttpInterceptor;
import com.aliyun.datahub.client.http.HttpRequest;
import com.aliyun.datahub.client.http.HttpResponse;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.core.HttpHeaders;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.*;

public class DatahubInterceptorHandler extends HttpInterceptor {
    private final static Logger LOGGER = LoggerFactory.getLogger(DatahubInterceptorHandler.class);
    private Account account;
    private String userAgent;
    private static String ipAddress;
    private static String clientId;

    static {
        ipAddress = getIpAddress();
        clientId = getClientId();
    }

    @Override
    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public DatahubInterceptorHandler(Account account) {
        this.account = account;
    }

    @Override
    public void preHandle(HttpRequest request) {
        addEssentialHeaders(request);
    }

    @Override
    public void postHandle(HttpResponse response) {
        try {
            String requestId = response.getHeader(DatahubConstant.X_DATAHUB_REQUEST_ID);
            if (requestId == null) {
                LOGGER.warn("requestid is null");
            }

            int status = response.getHttpStatus();
            if (status >= 400) {
                DatahubResponseError error;
                String contentType = response.getHeader(HttpHeaders.CONTENT_TYPE.toLowerCase());
                try {
                    if ("application/json".equalsIgnoreCase(contentType)) {
                        error = response.getEntity(DatahubResponseError.class);
                        requestId = (requestId == null ? error.getRequestId() : requestId);
                    } else {
                        error = new DatahubResponseError();
                        error.setMessage(response.getEntity(String.class));
                    }
                } catch (Exception e) {
                    error = new DatahubResponseError();
                    error.setMessage(e.getMessage());
                } finally {
                    response.close();
                }

                throw new DatahubClientException(status,
                        requestId,
                        error == null ? "" : error.getCode(),
                        error == null ? "" : error.getMessage());

            }
        } catch (ClientErrorException ex) {
            throw new DatahubClientException(ex.getMessage());
        }
    }

    private void addEssentialHeaders(HttpRequest request) {
        Map<String, String> headers = request.getHeaders();
        if (!headers.containsKey(DatahubConstant.X_DATAHUB_CLIENT_VERSION)) {
            request.header(DatahubConstant.X_DATAHUB_CLIENT_VERSION, DatahubConstant.DATAHUB_CLIENT_VERSION_1);
        }

        if (!headers.containsKey(HttpHeaders.DATE)) {
            request.header(HttpHeaders.DATE, DateFormat.getDateTimeFormat().format(new Date()));
        }

        // add content-type if necessary
        if (request.getEntity() != null) {
            request.header(HttpHeaders.CONTENT_TYPE, request.getEntity().getContentType());
        }

        if (ipAddress != null) {
            request.header(DatahubConstant.X_DATAHUB_SOURCE_IP, ipAddress);
        }

        String clientAgent = null;
        if (clientId != null) {
            clientAgent = (userAgent == null ? clientId : clientId + "-" + userAgent);
        } else if (userAgent != null) {
            clientAgent = userAgent;
        }
        if (clientAgent != null) {
            request.header(HttpHeaders.USER_AGENT, clientAgent);
        }

        // add authorization related headers
        account.addAuthHeaders(request);
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

    private static String getIpAddress() {
        String ip = null;
        Enumeration<NetworkInterface> interfaces = null;
        try {
            interfaces = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }

        while(interfaces.hasMoreElements()){
            NetworkInterface element = interfaces.nextElement();
            Enumeration<InetAddress> addresses = element.getInetAddresses();
            while (addresses.hasMoreElements()){
                InetAddress address = addresses.nextElement();
                if (address.isLoopbackAddress()) continue;
                if (address instanceof Inet4Address){
                    ip = address.getHostAddress();
                }
            }
        }
        return ip;
    }

    private static String getClientId() {
        // version
        String path = "META-INF/maven/com.aliyun.datahub/aliyun-sdk-datahub/pom.properties";
        Properties p = new Properties();
        try {
            p.load(DatahubInterceptorHandler.class.getClassLoader().getResourceAsStream(path));
        } catch (Throwable ignored) {
        }
        String versionNumber = p.getProperty("version", "Unknown");

        // pid
        String processName = java.lang.management.ManagementFactory
                .getRuntimeMXBean().getName();
        String processId = processName.substring(0, processName.indexOf('@'));
        return String.format(Locale.ENGLISH, "%s@%s-(Java-%s)", ipAddress, processId, versionNumber);
    }

    public Account getAccount() {
        return account;
    }
}
