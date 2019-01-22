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

package com.aliyun.datahub.rest;

import com.aliyun.datahub.auth.Account;
import com.aliyun.datahub.common.transport.*;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.common.util.RetryUtil;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.model.compress.Compression;
import com.aliyun.datahub.model.compress.CompressionFormat;
import com.aliyun.datahub.model.serialize.JsonErrorParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * RESTful API客户端
 */
public class RestClient {

    private Logger LOG = LoggerFactory.getLogger(RestClient.class);

    public static abstract class RetryLogger {

        /**
         * 当 RestClent 发生重试前的回调函数
         *
         * @param e              错误异常
         * @param retryCount     重试计数
         * @param retrySleepTime 下次需要的重试时间
         */
        public abstract void onRetryLog(Throwable e, long retryCount, long retrySleepTime);
    }

    /**
     * 底层网络建立超时时间,50秒
     */
    public static final int DEFAULT_CONNECT_TIMEOUT = 5; // seconds

    /**
     * 底层网络重试次数, 3
     */
    public static final int DEFAULT_CONNECT_RETRYTIMES = 4;

    /**
     * 底层网络连接超时时间, 120秒。
     */
    public static final int DEFAULT_READ_TIMEOUT = 120;// seconds

    /**
     * 是否忽略HTTPS证书验证
     */
    public static final boolean DEFAULT_IGNORE_CERTS = false;

    private final Transport transport;

    private Account account;
    private String endpoint;
    private boolean ignoreCerts = DEFAULT_IGNORE_CERTS;

    private static final String USER_AGENT_PREFIX = "JavaSDK Version " + "";

    private String userAgent;

    private CompressionFormat compressionFormat;

    private String sourceIp;
    private Boolean secureTransport;

    public RetryLogger getRetryLogger() {
        return logger;
    }

    public void setRetryLogger(RetryLogger logger) {
        this.logger = logger;
    }

    private RetryLogger logger = null;

    private boolean enableP2P = false;
    private String serverEndpoint = null;
    private int requestCount = 1000;

    /**
     * 创建RestClient对象
     *
     * @param transport
     */
    public RestClient(Transport transport, CompressionFormat compressionFormat) {
        this.transport = transport;
        this.compressionFormat = compressionFormat;
    }

//    /**
//     * 请求RESTful API
//     *
//     * @param clazz    返回结果绑定的Java类型
//     * @param resource API资源标识
//     * @param method   访问方法
//     * @return 与API返回结果绑定的clazz类型对象
//     * @throws DatahubException
//     */
//    public <T> T request(Class<T> clazz, String resource, String method) throws DatahubException {
//        return request(clazz, resource, method, null, null, null);
//    }
//
//    /**
//     * 请求RESTful API
//     *
//     * @param clazz
//     * @param resource
//     * @param method
//     * @param params
//     * @return
//     * @throws DatahubException
//     */
//    public <T> T request(Class<T> clazz, String resource, String method, Map<String, String> params)
//            throws DatahubException {
//        return request(clazz, resource, method, params, null, null);
//    }

    private static final String CHARSET = "UTF-8";

//    /**
//     * 请求RESTful API
//     *
//     * @param clazz
//     * @param resource
//     * @param method
//     * @param params
//     * @param headers
//     * @param body
//     * @return
//     * @throws DatahubException
//     */
//    public <T> T stringRequest(Class<T> clazz, String resource, String method,
//                               Map<String, String> params,
//                               Map<String, String> headers, String body) throws DatahubException {
//        try {
//            return request(clazz, resource, method, params, headers, body.getBytes(CHARSET));
//        } catch (UnsupportedEncodingException e) {
//            throw new DatahubException(e.getMessage(), e);
//        }
//    }
//
//    /**
//     * 请求RESTful API
//     *
//     * @param clazz
//     * @param resource
//     * @param method
//     * @param params
//     * @param headers
//     * @param body
//     * @return
//     * @throws DatahubException
//     */
//    public <T> T request(Class<T> clazz, DefaultRequest request) throws DatahubException {
//        T r = null;
//        Response resp = request(request);
//        try {
//            r = JAXBUtils.unmarshal(resp, clazz);
//        } catch (JAXBException e) {
//            throw new DatahubException("Can't bind xml to " + clazz.getName(), e);
//        }
//
//        return r;
//    }
//
//    /**
//     * 请求RESTful API
//     *
//     * @param resource
//     * @param method
//     * @param params
//     * @param headers
//     * @param body
//     * @return
//     * @throws DatahubException
//     */
//    public Response stringRequest(String resource, String method, Map<String, String> params,
//                                  Map<String, String> headers, String body) throws DatahubException {
//        try {
//            return request(resource, method, params, headers, body.getBytes(CHARSET));
//        } catch (UnsupportedEncodingException e) {
//            throw new DatahubException(e.getMessage(), e);
//        }
//
//    }
//
//    /**
//     * 请求RESTful API
//     *
//     * @param resource
//     * @param method
//     * @param params
//     * @param headers
//     * @param body
//     * @return
//     * @throws DatahubException
//     */
//    public Response request(String resource, String method, Map<String, String> params,
//                            Map<String, String> headers,
//                            byte[] body) throws DatahubException {
//        if (null == body) {
//            return request(resource, method, params, headers, null, 0);
//        } else {
//            return request(resource, method, params, headers, new ByteArrayInputStream(body),
//                    body.length);
//        }
//    }

    /**
     * 带有重试的request
     *
     * @param request request实体
     * @return response
     */
    public Response request(final DefaultRequest request) {
        int retryTimes = 0;
        HttpMethod method = request.getHttpMethod();
        if (method == HttpMethod.GET || method == HttpMethod.HEAD) {
            retryTimes = getRetryTimes();
        }
        long retryWaitTime = getConnectTimeout() + getReadTimeout();
        try {
            return RetryUtil.executeWithRetry(new Callable<Response>() {
                @Override
                public Response call() throws Exception {
                    Response response = requestWithNoRetry(request);
                    if (response == null) {
                        throw new DatahubServiceException("Response is null.");
                    }
                    // IF THE HTTP CODE IS 4XX,
                    // IT SHOULD NOT RETRY IF THE REQUEST NOT CHANGED
                    if (response.getStatus() / 100 == 4) {
                        return response;
                    }
                    handleErrorResponse(response);
                    return response;
                }
            }, retryTimes, retryWaitTime * 1000, false);
        } catch (Exception e) {
            LOG.error("", e);
        }
        throw new DatahubServiceException("Failed in Connection Retry.");
    }

    /**
     * 解析response异常并抛出
     *
     * @param resp response
     */
    private void handleErrorResponse(Response resp) {
        if (!resp.isOK()) {
            ErrorMessage error = null;
            try {
                error = JAXBUtils.unmarshal(resp, ErrorMessage.class);
            } catch (Exception e) {
                //
            }

            DatahubServiceException e = null;
            if (resp.getStatus() == 404) {
                if (error != null) {
                    e = new DatahubServiceException(error.getMessage(), new RestException(error));
                }
            } else {
                if (error != null) {
                    e = new DatahubServiceException(error.getMessage(), new RestException(error));
                } else {
                    e = new DatahubServiceException(new String(resp.getBody()));
                    e.setRequestId(resp.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));
                }
            }
            throw e;
        }
    }

    /**
     * 没有重试的request
     *
     * @param request request实体
     * @return response
     */
    public Response requestWithNoRetry(DefaultRequest request) {
        return requestWithNoRetry(request, enableP2P);
    }

    /**
     * 没有重试的request
     *
     * @param request request实体
     * @param p2p p2p模式
     * @return response
     */
    public Response requestWithNoRetry(DefaultRequest request, boolean p2p) {
        if (compressionFormat != null && request.getBody() != null) {
            request.addHeader(Headers.CONTENT_ENCODING, compressionFormat.toString());
            if (compressionFormat.equals(CompressionFormat.LZ4)) {
                request.addHeader(DatahubHttpHeaders.HEADER_DATAHUB_CONTENT_RAW_SIZE, Integer.toString(request.getBody().length));
            }
            request.addHeader(Headers.ACCEPT_ENCODING, compressionFormat.toString());
            byte[] compressed = Compression.compress(request.getBody(), compressionFormat);
            request.setBody(compressed);
        }

        if (sourceIp != null && !sourceIp.isEmpty()) {
            request.addHeader(DatahubHttpHeaders.HEADER_DATAHUB_SOURCE_IP, sourceIp);
        }

        if (secureTransport != null) {
            request.addHeader(DatahubHttpHeaders.HEADER_DATAHUB_SECURE_TRANSPORT, secureTransport ? "true" : "false");
        }

        /**
         * 请求之前需要做签名
         */
        this.account.getRequestSigner().sign(request.getResource(), request);

        Response response = null;
        try {
            if (p2p) {
                updateRoute();
                response = transport.request(request, serverEndpoint);
            } else {
                response = transport.request(request);
            }
        } catch (IOException e) {
            throw new DatahubServiceException(e.getMessage(), e);
        }

        if (response.getBody() != null && response.getBody().length != 0) {
            String type = response.getHeaders().get(Headers.CONTENT_ENCODING);
            if (type != null && !type.isEmpty()) {
                try {
                    CompressionFormat format = CompressionFormat.fromValue(type);
                    if (format.equals(CompressionFormat.LZ4)) {
                        String rawSizeString = response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_CONTENT_RAW_SIZE);
                        if (rawSizeString == null || rawSizeString.isEmpty()) {
                            throw new DatahubServiceException("DecompressError", DatahubHttpHeaders.HEADER_DATAHUB_CONTENT_RAW_SIZE + "is missing.", response);
                        }
                        byte[] restored = Compression.decompress(response.getBody(), format, Integer.valueOf(rawSizeString));
                        response.setBody(restored);
                    } else {
                        byte[] restored = Compression.decompress(response.getBody(), format);
                        response.setBody(restored);
                    }
                } catch (RuntimeException e) {
                    throw new DatahubServiceException("DecompressError", e.getMessage(), response);
                }
            }
        }

        return response;
    }

    /**
     * @param request
     * @return
     * @throws IOException
     */
    public Connection connect(DefaultRequest request) throws IOException {
        if (sourceIp != null && !sourceIp.isEmpty()) {
            request.addHeader(DatahubHttpHeaders.HEADER_DATAHUB_SOURCE_IP, sourceIp);
        }

        if (secureTransport != null) {
            request.addHeader(DatahubHttpHeaders.HEADER_DATAHUB_SECURE_TRANSPORT, secureTransport ? "true" : "false");
        }

        this.account.getRequestSigner().sign(request.getResource(), request);
        return transport.connect(request);
    }

    public void setAccount(Account account) {
        this.account = account;
    }

    public Account getAccount() {
        return account;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public Transport getTransport() {
        return transport;
    }

//    private DefaultRequest buildRequest(String resource, String method, Map<String, String> params,
//                                        Map<String, String> headers)
//            throws DatahubException {
//        if (resource == null || !resource.startsWith("/")) {
//            throw new DatahubException("Invalid resource: " + resource);
//        }
//
//        if (endpoint == null) {
//            throw new DatahubException("Odps endpoint required.");
//        }
//
//        DefaultRequest req = new DefaultRequest(this);
//
//        // build URL with parameters
//        StringBuilder url = new StringBuilder();
//        url.append(endpoint).append(resource);
//
//        if (params == null) {
//            params = new HashMap<String, String>();
//        }
//
//        if (params.size() != 0) {
//            req.setParameters(params);
//
//            url.append('?');
//            boolean first = true;
//            for (Entry<String, String> kv : params.entrySet()) {
//
//                if (first) {
//                    first = false;
//                } else {
//                    url.append('&');
//                }
//
//                String key = kv.getKey();
//                String value = kv.getValue();
//                url.append(key);
//                if (value != null && value.length() > 0) {
//                    value = ResourceBuilder.encode(value);
//                    url.append('=').append(value);
//                }
//            }
//        }
//
//        try {
//
//            req.setURI(new URI(url.toString()));
//            req.setMethod(Method.valueOf(method));
//
//            if (headers != null) {
//                req.setHeaders(headers);
//            }
//
//            // set User-Agent
//            if (req.getHeaders().get(Headers.USER_AGENT) == null && userAgent != null) {
//                req.setHeader(Headers.USER_AGENT, userAgent);
//                req.setHeader("x-datahub-user-agent", userAgent);
//            }
//
//            req.getHeaders().put("Date", DateUtils.formatRfc822Date(new Date()));
//
//            // Sign the request
//            //account.getRequestSigner().sign(resource, req);
//        } catch (URISyntaxException e) {
//            throw new DatahubException(e.getMessage(), e);
//        }
//
//        return req;
//    }

    /**
     * 设置User-Agent
     *
     * @param userAgent
     */
    public void setUserAgent(String userAgent) {
        this.userAgent = (USER_AGENT_PREFIX + " " + userAgent).trim();
    }

    int connectTimeout = DEFAULT_CONNECT_TIMEOUT;

    /**
     * 设置建立连接超时时间
     *
     * @param timeout 超时时间，单位秒
     */
    public void setConnectTimeout(int timeout) {
        this.connectTimeout = timeout;
    }

    /**
     * 获取建立连接超时时间
     *
     * @return 超时时间，单位秒
     */
    public int getConnectTimeout() {
        return connectTimeout;
    }

    int readTimeout = DEFAULT_READ_TIMEOUT;

    /**
     * 设置网络超时时间
     *
     * @param timeout 超时时间，单位秒
     */
    public void setReadTimeout(int timeout) {
        this.readTimeout = timeout;
    }

    /**
     * 获取建立网络超时时间
     *
     * @return 超时时间，单位秒
     */
    public int getReadTimeout() {
        return readTimeout;
    }

    int retryTimes = DEFAULT_CONNECT_RETRYTIMES;

    /**
     * 获取网络重试次数
     *
     * @return 重试次数
     */
    public int getRetryTimes() {
        return retryTimes;
    }


    /**
     * 设置网络重试次数
     *
     * @param retryTimes 重试次数
     */
    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    /**
     * 获取是否忽略 Https 验证
     *
     * @return
     */
    public boolean isIgnoreCerts() {
        return ignoreCerts;
    }

    /**
     * 设置是否忽略 Https 验证
     *
     * @param ignoreCerts
     */
    public void setIgnoreCerts(boolean ignoreCerts) {
        this.ignoreCerts = ignoreCerts;
    }

    public String getSourceIp() {
        return sourceIp;
    }

    public void setSourceIp(String sourceIp) {
        this.sourceIp = sourceIp;
    }

    public boolean getSecureTransport() {
        return secureTransport;
    }

    public void setSecureTransport(boolean secureTransport) {
        this.secureTransport = secureTransport;
    }

    public void close() {
        transport.close();
    }

    public void enableP2P(boolean enabled) {
        this.enableP2P = enabled;
    }

    private void updateRoute() {
        if (requestCount >= 1000) {
            try {
                DefaultRequest req = new DefaultRequest();
                req.setHttpMethod(HttpMethod.GET);
                req.setResource("/system/status");

                Response resp = requestWithNoRetry(req, false);
                if (!resp.isOK()) {
                    throw JsonErrorParser.getInstance().parse(resp);
                }

                ObjectMapper mapper = JacksonParser.getObjectMapper();

                // convert JSON string to Map
                Map<String, String> map = mapper.readValue(resp.getBody(), new TypeReference<Map<String, String>>() {
                });

                String serverIp = map.get("IP");
                if (serverIp != null) {
                    serverEndpoint = "http://" + serverIp;
                }
                requestCount = 0;
            } catch (IOException e) {
                // ignore
            }
        } else {
            ++requestCount;
        }
    }
}
