package com.aliyun.datahub.client.http;

import com.aliyun.datahub.client.auth.Account;
import com.aliyun.datahub.client.http.converter.EmptyBodyConverterFactory;
import com.aliyun.datahub.client.http.converter.ProtobufConverterFactory;
import com.aliyun.datahub.client.http.interceptor.InterceptorWrapper;
import com.aliyun.datahub.client.http.interceptor.compress.DeflateRequestInterceptor;
import com.aliyun.datahub.client.http.interceptor.compress.Lz4RequestInterceptor;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import okhttp3.logging.HttpLoggingInterceptor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public abstract class HttpClient {
    private final static Logger LOGGER = LoggerFactory.getLogger(HttpClient.class);
    private static ConnectionPool connectionPool;
    private static int maxIdleConnections = 3;

    private static ObjectMapper DEFAULT_MAPPER;
    private static Map<ClientConfig, Retrofit> clientMap = new HashMap<>();

    static {
        DEFAULT_MAPPER = new ObjectMapper();
        DEFAULT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        DEFAULT_MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        DEFAULT_MAPPER.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
    }

    public static Retrofit createClient(String endpoint, HttpConfig config) {
        return createClient(endpoint, config, new InterceptorWrapper());
    }

    public static void close() {
        if (connectionPool != null) {
            connectionPool.evictAll();
        }
    }

    public static Retrofit createClient(String endpoint, HttpConfig config, InterceptorWrapper wrapper) {
        if (!endpoint.startsWith("http")) {
            endpoint = "http://" + endpoint;
        }
        if (!endpoint.endsWith("/")) {
            endpoint += "/";
        }

        ClientConfig clientConfig = new ClientConfig(endpoint, config, wrapper.getAuth().getAccount());
        Retrofit retrofit = clientMap.get(clientConfig);
        if (retrofit == null) {
            synchronized (HttpClient.class) {
                retrofit = clientMap.get(clientConfig);
                if (retrofit == null) {
                    retrofit = newClient(endpoint, config, wrapper);
                    clientMap.put(clientConfig, retrofit);
                }
            }
        }
        return retrofit;
    }

    private static Retrofit newClient(String endpoint, HttpConfig config, InterceptorWrapper wrapper) {
        return new Retrofit.Builder()
                .client(buildClient(config, wrapper))
                .addConverterFactory(EmptyBodyConverterFactory.create())
                .addConverterFactory(ProtobufConverterFactory.create(config.isEnablePbCrc()))
                .addConverterFactory(JacksonConverterFactory.create(DEFAULT_MAPPER))
                .baseUrl(endpoint)
                .build();
    }

    private static OkHttpClient buildClient(HttpConfig config, InterceptorWrapper wrapper) {
        if (connectionPool == null) {
            connectionPool = new ConnectionPool(maxIdleConnections, 60, TimeUnit.SECONDS);
        }
        OkHttpClient.Builder builder = new OkHttpClient.Builder()
                .connectionPool(connectionPool)
                .readTimeout(config.getReadTimeout(), TimeUnit.MILLISECONDS)
                .writeTimeout(config.getReadTimeout(), TimeUnit.MILLISECONDS)
                .connectTimeout(config.getConnTimeout(), TimeUnit.MILLISECONDS)
                .sslSocketFactory(sslSocketFactory(), x509TrustManager())
                .addInterceptor(wrapper.getResponse());

        if (config.isEnableH2C()) {
            builder.protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE));
        }

        if (config.getCompressType() == HttpConfig.CompressType.DEFLATE) {
            builder.addInterceptor(new DeflateRequestInterceptor());
        } else if (config.getCompressType() == HttpConfig.CompressType.LZ4) {
            builder.addInterceptor(new Lz4RequestInterceptor());
        }

        builder.addInterceptor(wrapper.getAuth());

        if (!StringUtils.isEmpty(config.getProxyUri())) {
            String proxyUri = config.getProxyUri();
            int index = proxyUri.lastIndexOf(":");
            String proxyHost = proxyUri.substring(0, index);
            String proxyPortStr = proxyUri.substring(index + 1);
            int proxyPort = StringUtils.isEmpty(proxyPortStr) ? 80 : Integer.valueOf(proxyPortStr);
            builder.proxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort)))
                    .proxyAuthenticator(new Authenticator() {
                        @Override
                        public Request authenticate(Route route, Response response) throws IOException {
                            String credential = Credentials.basic(config.getProxyUsername(), proxyHost);
                            return response.request().newBuilder()
                                    .header("Proxy-Authorization", credential)
                                    .build();
                        }
                    });
        }

        if (config.isDebugRequest()) {
            builder.addInterceptor(loggingInterceptor());
        }

        return builder.build();
    }

    private static HttpLoggingInterceptor loggingInterceptor() {
        HttpLoggingInterceptor.Logger CUSTOM = new HttpLoggingInterceptor.Logger() {
            @Override
            public void log(String message) {
                LOGGER.info("DataHubClient: " + message);
            }
        };
        HttpLoggingInterceptor loggingInterceptor = new HttpLoggingInterceptor(CUSTOM);
        loggingInterceptor.setLevel(HttpLoggingInterceptor.Level.BODY);
        return loggingInterceptor;
    }

    private static X509TrustManager x509TrustManager() {
        return new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
            }
            @Override
            public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
            }
            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        };
    }

    private static SSLSocketFactory sslSocketFactory() {
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[]{x509TrustManager()}, new SecureRandom());
            return sslContext.getSocketFactory();
        } catch (GeneralSecurityException ex) {
            LOGGER.warn(ex.toString());
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    public static int getMaxIdleConnections() {
        return maxIdleConnections;
    }

    public static void setMaxIdleConnections(int maxIdleConnections) {
        HttpClient.maxIdleConnections = maxIdleConnections;
    }

    private static class ClientConfig {
        private String endpoint;
        private HttpConfig config;
        private Account account;

        public ClientConfig(String endpoint, HttpConfig config, Account account) {
            this.endpoint = endpoint;
            this.config = config;
            this.account = account;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ClientConfig that = (ClientConfig) o;
            return Objects.equals(endpoint, that.endpoint) &&
                    Objects.equals(config, that.config) &&
                    Objects.equals(account, that.account);
        }

        @Override
        public int hashCode() {
            return Objects.hash(endpoint, config, account);
        }
    }
}
