package com.aliyun.datahub.client.http;

import com.aliyun.datahub.client.auth.Account;
import com.aliyun.datahub.client.http.compress.lz4.LZ4EncoderFilter;
import com.aliyun.datahub.client.http.compress.lz4.LZ4EncoderInterceptor;
import com.aliyun.datahub.client.http.interceptor.AuthInterceptor;
import com.aliyun.datahub.client.http.interceptor.MetricInterceptor;
import com.aliyun.datahub.client.http.provider.JsonContextProvider;
import com.aliyun.datahub.client.http.provider.ProtobufMessageProvider;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.glassfish.jersey.apache.connector.ApacheClientProperties;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.RequestEntityProcessing;
import org.glassfish.jersey.client.filter.EncodingFilter;
import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.message.DeflateEncoder;
import org.glassfish.jersey.message.GZipEncoder;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public abstract class HttpClient {
    private static final Logger LOGGER = Logger.getLogger(HttpClient.class.getName());
    private static Map<ClientInfo, Client> clientMap = new HashMap<>();
    private static ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
    private static ScheduledFuture<?> fTask;

    static {
        fTask = executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                for (Client client : clientMap.values()) {
                    PoolingHttpClientConnectionManager connManager = (PoolingHttpClientConnectionManager) client
                            .getConfiguration().getProperty(ApacheClientProperties.CONNECTION_MANAGER);
                    connManager.closeExpiredConnections();
                }
            }
        }, 500, 10000, TimeUnit.MILLISECONDS);
    }

    public static void close() {
        fTask.cancel(true);
        executorService.shutdown();

        for (Client client : clientMap.values()) {
            PoolingHttpClientConnectionManager poolMngr = (PoolingHttpClientConnectionManager) client.getConfiguration()
                    .getProperty(ApacheClientProperties.CONNECTION_MANAGER);
            poolMngr.close();
        }
    }

    public static HttpRequest createRequest(String endpoint, HttpConfig config) {
        return createRequest(endpoint, config, new HttpInterceptor());
    }

    public static HttpRequest createRequest(String endpoint, HttpConfig config, HttpInterceptor interceptor) {
        if (!endpoint.startsWith("http")) {
            endpoint = "http://" + endpoint;
        }

        ClientInfo clientInfo = new ClientInfo(config, interceptor.getAccount());
        Client client = clientMap.get(clientInfo);
        if (client == null) {
            synchronized (HttpClient.class) {
                client = clientMap.get(clientInfo);
                if (client == null) {
                    client = createClient(config, interceptor.getAccount());
                    clientMap.put(clientInfo, client);
                }
            }
        }

        return new HttpRequest(client)
                .endpoint(endpoint)
                .interceptor(interceptor)
                .maxRetryCount(config.getMaxRetryCount());
    }

    private static Client createClient(HttpConfig config, Account account) {
        ClientConfig clientConfig = getClientConfig(config);
        Client client = ClientBuilder.newBuilder()
                .withConfig(clientConfig)
                .build();

        client.property(ClientProperties.REQUEST_ENTITY_PROCESSING, RequestEntityProcessing.BUFFERED);
        client.property(ClientProperties.READ_TIMEOUT, config.getReadTimeout());
        client.property(ClientProperties.CONNECT_TIMEOUT, config.getConnTimeout());
        client.property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true);

        if (config.getProxyUri() != null && !config.getProxyUri().isEmpty()) {
            client.property(ClientProperties.PROXY_URI, config.getProxyUri());
            client.property(ClientProperties.PROXY_USERNAME, config.getProxyUsername());
            client.property(ClientProperties.PROXY_PASSWORD, config.getProxyPassword());
        }

        client.register(new JacksonFeature())
                .register(JsonContextProvider.class);
        client.register(new ProtobufMessageProvider(config.isEnablePbCrc()));

        // As raw-size is needed for auth, so should auth request in interceptor
        client.register(new AuthInterceptor(account), Priorities.AUTH);

        // register encoders
        if (config.getCompressType() != null) {
            if (config.getCompressType() == HttpConfig.CompressType.LZ4) {
                client.register(LZ4EncoderInterceptor.class, Priorities.ENCODER);
                client.register(LZ4EncoderFilter.class);
            } else {
                client.register(DeflateEncoder.class, Priorities.ENCODER);
                client.register(GZipEncoder.class, Priorities.ENCODER);
                client.register(EncodingFilter.class);
                client.property(ClientProperties.USE_ENCODING, config.getCompressType().getValue());
            }
        }

        // for metric tps
        client.register(new MetricInterceptor(), Priorities.METRIC);

        if (config.isDebugRequest()) {
            client.register(new LoggingFilter(LOGGER, true));
        }

        return client;
    }

    private static ClientConfig getClientConfig(HttpConfig config) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.connectorProvider(new ApacheConnectorProvider());
        PoolingHttpClientConnectionManager connectionManager =
                new PoolingHttpClientConnectionManager(
                        getSocketFactoryRegistry(),
                        null,
                        null,
                        null,
                        60,
                        TimeUnit.SECONDS);

        connectionManager.setMaxTotal(config.getMaxConnTotal());
        connectionManager.setDefaultMaxPerRoute(config.getMaxConnPerRoute());
        clientConfig.property(ApacheClientProperties.CONNECTION_MANAGER, connectionManager);
        return clientConfig;
    }


    private static Registry<ConnectionSocketFactory> getSocketFactoryRegistry() {
        TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager(){
            public void checkClientTrusted(X509Certificate[] arg0, String arg1)
                    throws CertificateException {
            }
            public void checkServerTrusted(X509Certificate[] arg0, String arg1)
                    throws CertificateException {
            }
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        }};

        SSLContext sslContext;
        try {
            sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
        } catch (java.security.GeneralSecurityException ex) {
            LOGGER.warning(ex.toString());
            throw new RuntimeException(ex.getMessage(), ex);
        }

        SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
        return RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.INSTANCE)
                .register("https", sslsf)
                .build();
    }

    private static class ClientInfo {
        private HttpConfig config;
        private Account account;

        public ClientInfo(HttpConfig config, Account account) {
            this.config = config;
            this.account = account;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ClientInfo that = (ClientInfo) o;
            return Objects.equals(config, that.config) &&
                    Objects.equals(account, that.account);
        }

        @Override
        public int hashCode() {
            return Objects.hash(config, account);
        }
    }
}
