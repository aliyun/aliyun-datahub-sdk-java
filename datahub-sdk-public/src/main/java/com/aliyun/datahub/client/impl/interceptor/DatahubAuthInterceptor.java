package com.aliyun.datahub.client.impl.interceptor;

import com.aliyun.datahub.client.auth.Account;
import com.aliyun.datahub.client.common.DatahubConstant;
import com.aliyun.datahub.client.common.DateFormat;
import com.aliyun.datahub.client.http.interceptor.AuthInterceptor;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.http.HttpHeaders;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.*;

public class DatahubAuthInterceptor extends AuthInterceptor {
    private String userAgent;
    private String ipAddress;
    private String clientId;

    public DatahubAuthInterceptor(Account account, String userAgent) {
        super(account);
        this.userAgent = userAgent;
        ipAddress = getIpAddress();
        clientId = getClientId();
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request request = chain.request();
        Request.Builder reqBuilder = request.newBuilder();
        addEssentialHeaders(request, reqBuilder);
        return chain.proceed(reqBuilder.build());
    }

    private void addEssentialHeaders(Request request, Request.Builder reqBuilder) {
        Set<String> headerNames = request.headers().names();
        if (!headerNames.contains(DatahubConstant.X_DATAHUB_CLIENT_VERSION)) {
            reqBuilder.addHeader(DatahubConstant.X_DATAHUB_CLIENT_VERSION, DatahubConstant.DATAHUB_CLIENT_VERSION_1);
        }

        if (!headerNames.contains(HttpHeaders.DATE)) {
            reqBuilder.addHeader(HttpHeaders.DATE, DateFormat.getDateTimeFormat().format(new Date()));
        }

        // as auth with Content-Type, here get it for signature
        RequestBody body = request.body();
        if (body != null) {
            MediaType contentType = body.contentType();
            if (contentType != null) {
                reqBuilder.addHeader(HttpHeaders.CONTENT_TYPE, contentType.toString());
            }
        }

        if (ipAddress != null) {
            reqBuilder.addHeader(DatahubConstant.X_DATAHUB_SOURCE_IP, ipAddress);
        }

        String clientAgent = null;
        if (clientId != null) {
            clientAgent = (userAgent == null ? clientId : clientId + "-" + userAgent);
        } else if (userAgent != null) {
            clientAgent = userAgent;
        }
        if (clientAgent != null) {
            reqBuilder.addHeader(HttpHeaders.USER_AGENT, clientAgent);
        }

        // add authorization related headers
        account.addAuthHeaders(reqBuilder);
    }

    private String getIpAddress() {
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

    private String getClientId() {
        // version
        String path = "META-INF/maven/com.aliyun.datahub/aliyun-sdk-datahub/pom.properties";
        Properties p = new Properties();
        try {
            p.load(DatahubAuthInterceptor.class.getClassLoader().getResourceAsStream(path));
        } catch (Throwable ignored) {
        }
        String versionNumber = p.getProperty("version", "Unknown");

        // pid
        String processName = java.lang.management.ManagementFactory
                .getRuntimeMXBean().getName();
        String processId = processName.substring(0, processName.indexOf('@'));
        return String.format(Locale.ENGLISH, "%s@%s-(Java-%s)", ipAddress, processId, versionNumber);
    }
}
