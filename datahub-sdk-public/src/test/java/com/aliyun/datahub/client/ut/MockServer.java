package com.aliyun.datahub.client.ut;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.StringBody;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import java.nio.charset.StandardCharsets;

public class MockServer {
    protected final static String LOCALHOST = "127.0.0.1";

    protected static ClientAndServer mockServer;
    protected static MockServerClient mockServerClient;
    protected static DatahubClient client;
    protected static int listenPort;
    protected static String serverEndpoint;

    @BeforeClass
    public static void setUp() {
        mockServer = ClientAndServer.startClientAndServer(0);
        listenPort = mockServer.getPort();
        serverEndpoint = LOCALHOST + ":" + listenPort;

        mockServerClient = new MockServerClient(LOCALHOST, listenPort);

        client = DatahubClientBuilder.newBuilder().setDatahubConfig(
                new DatahubConfig(
                        serverEndpoint,
                        new AliyunAccount("test_ak", "test_sk", "test_token"),
                        false
                )
        ).build();
    }

    @BeforeMethod
    public void clear() {
        mockServer.reset();
    }

    @AfterClass
    public static void tearDown() {
        mockServer.stop();
    }

    protected static StringBody exact(String body) {
        return StringBody.exact(body, StandardCharsets.UTF_8);
    }

}