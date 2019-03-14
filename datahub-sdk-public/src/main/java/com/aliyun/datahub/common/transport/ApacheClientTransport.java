package com.aliyun.datahub.common.transport;

import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.exception.DatahubClientException;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ApacheClientTransport implements Transport {
    private static final Logger log = Logger.getLogger(ApacheClientTransport.class.getName());
    private HttpClient httpClient;
    private DatahubConfiguration conf;

    public ApacheClientTransport(DatahubConfiguration conf) {
        this.conf = conf;

        httpClient = new HttpClient();
        httpClient.setConnectionTimeout(conf.getSocketConnectTimeout() * 1000);
        httpClient.setSoTimeout(conf.getSocketTimeout() * 1000);
        httpClient.setMaxRouteConnections(conf.getConnectionsPerEndpoint());
        httpClient.setTotalConn(conf.getTotalConnections());
        try {
            httpClient.init();
        } catch (NoSuchAlgorithmException e) {
            throw new DatahubClientException("create http client error", e);
        } catch (KeyStoreException e) {
            throw new DatahubClientException("create http client error", e);
        } catch (KeyManagementException e) {
            throw new DatahubClientException("create http client error", e);
        }
    }

    @Override
    public Response request(DefaultRequest req, String endpoint) throws IOException {
        String dhEndpoint = conf.getEndpoint();
        if (endpoint != null && !endpoint.isEmpty()) {
            dhEndpoint = endpoint;
        }
        String url = dhEndpoint + req.getResource();
        if (log.isLoggable(Level.FINE)) {
            log.fine("Connectiong to " + url);
        }

        if (log.isLoggable(Level.FINE)) {
            log.fine("Request headers: " + req.getHeaders().toString());
        }

        HttpClient.HttpResult result = null;

        switch (req.getHttpMethod()) {
            case POST:
                result = httpClient.doPostWithBytes(url, req.getHeaders(), req.getBody());
                break;
            case PUT:
                result = httpClient.doPutWithBytes(url, req.getHeaders(), req.getBody());
                break;
            case GET:
                result = httpClient.doGet(url, req.getHeaders());
                break;
            case DELETE:
                result = httpClient.doDelete(url, req.getHeaders());
                break;
            case HEAD:
                result = httpClient.doHead(url, req.getHeaders());
                break;
        }

        if (result == null) {
            throw new IOException("request to server failed");
        }
        DefaultResponse response = new DefaultResponse();
        response.setStatus(result.getCode());
        if (result.getHeader() != null) {
            for (Map.Entry<String, String> entry : result.getHeader().entrySet()) {
                response.setHeader(entry.getKey(), entry.getValue());
            }
        }
        response.setBody(result.getBody());

        return response;
    }

    @Override
    public Response request(DefaultRequest req) throws IOException {
        return request(req, null);
    }

    @Override
    public Connection connect(DefaultRequest req) throws IOException {
        throw new DatahubClientException("not implemented");
    }

    @Override
    public void close() {
        httpClient.shutdown();
    }
}
