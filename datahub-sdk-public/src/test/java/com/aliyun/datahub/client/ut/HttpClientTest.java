package com.aliyun.datahub.client.ut;

import com.aliyun.datahub.client.http.*;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.glassfish.jersey.apache.connector.ApacheClientProperties;
import org.mockserver.model.Header;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.ws.rs.ProcessingException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class HttpClientTest extends MockServer {
    static class InnerClass {
        private String code;
        private String message;

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

    @Test
    public void testJsonMediaType() {
        mockServerClient.when(request()).respond(
                response().withHeader("Content-Type", "application/json")
                .withBody("{\"code\":\"testCode\",\"message\":\"testMessage\"}")
        );

        HttpResponse response = HttpClient.createRequest(serverEndpoint, new HttpConfig())
                .method(HttpMethod.GET).path("/get").fetchResponse();

        InnerClass innerClass = response.getEntity(InnerClass.class);
        Assert.assertEquals("testCode", innerClass.getCode());
        Assert.assertEquals("testMessage", innerClass.getMessage());
    }

    @Test
    public void testTextMediaType() {
        mockServerClient.when(request()).respond(
                response().withHeader("Content-Type", "text/plain")
                        .withBody("{\"code\":\"testCode\",\"message\":\"testMessage\"}")
        );

        HttpResponse response = HttpClient.createRequest(serverEndpoint, new HttpConfig())
                .method(HttpMethod.GET).path("/get").fetchResponse();

        InnerClass innerClass = response.getEntity(InnerClass.class);
        Assert.assertNull(innerClass);

        response = HttpClient.createRequest(serverEndpoint, new HttpConfig())
                .method(HttpMethod.GET).path("/get").fetchResponse();
        String result = response.getEntity(String.class);
        Assert.assertEquals("{\"code\":\"testCode\",\"message\":\"testMessage\"}", result);
    }

    @Test
    public void testUpsupportMedieType() {
        mockServerClient.when(request()).respond(
                response().withHeader("Content-Type", "text/plain")
                .withBody("TestResult")
        );

        HttpResponse response = HttpClient.createRequest(serverEndpoint, new HttpConfig())
                .method(HttpMethod.GET).path("/get").fetchResponse();
        String result = response.getEntity(String.class);
        Assert.assertEquals(result, "TestResult");
        Assert.assertNull(result);
    }

    @Test
    public void testConnectRefuse() {
        tearDown();

        try {
            HttpResponse response = HttpClient.createRequest(serverEndpoint, new HttpConfig()).method(HttpMethod.GET).path("/get").fetchResponse();
            fail("call closed server with no exception throw");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof HttpHostConnectException);
        }

        setUp();
    }

    @Test
    public void testReadTimeout() {

        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withDelay(TimeUnit.SECONDS, 5)
        );

        try {
            HttpResponse response = HttpClient.createRequest(serverEndpoint, new HttpConfig().setReadTimeout(1000))
                    .method(HttpMethod.GET).path("/get").fetchResponse();
            fail("delay call with no exception throw");
        } catch (ProcessingException e) {
            Assert.assertTrue(e.getCause() instanceof SocketTimeoutException);
        }
    }

    private static final int MAX_CONN_PER_ROUTE = 5;
    @Test
    public void testConnPool() {
        HttpConfig defaultConfig = new HttpConfig().setMaxConnPerRoute(MAX_CONN_PER_ROUTE).setMaxConnTotal(MAX_CONN_PER_ROUTE);

        mockServerClient.when(
                request().withMethod("GET").withPath("/get")
        ).respond(
                response().withStatusCode(200)
                        .withHeader(new Header("Content-Type", "application/json"))
                        .withBody("ReturnMessage")
                        .withDelay(TimeUnit.SECONDS, 1)
        );

        List<TestThread> threadList = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            TestThread testThread = new TestThread(defaultConfig);
            threadList.add(testThread);
            testThread.start();
        }

        HttpRequest request = HttpClient.createRequest(LOCALHOST + listenPort, defaultConfig);
        PoolingHttpClientConnectionManager connectionManager = (PoolingHttpClientConnectionManager) request.getClient()
                .getConfiguration().getProperty(ApacheClientProperties.CONNECTION_MANAGER);
        Assert.assertTrue(connectionManager.getTotalStats().getLeased() <= MAX_CONN_PER_ROUTE);

        for (TestThread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                //
            }
        }
    }

    private static class TestThread extends Thread {
        private HttpConfig config;

        public TestThread(HttpConfig config) {
            this.config = config;
        }

        @Override
        public void run() {
            try {
                String result = HttpClient.createRequest(serverEndpoint, config).path("/get").get(String.class);

                Assert.assertEquals("ReturnMessage", result);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}
