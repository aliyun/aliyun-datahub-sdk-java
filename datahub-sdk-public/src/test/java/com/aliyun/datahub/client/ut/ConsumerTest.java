package com.aliyun.datahub.client.ut;

import com.aliyun.datahub.client.model.HeartbeatResult;
import com.aliyun.datahub.client.model.JoinGroupResult;
import com.aliyun.datahub.client.model.LeaveGroupResult;
import com.aliyun.datahub.client.model.SyncGroupResult;
import org.mockserver.model.Header;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class ConsumerTest extends MockServer {
    @Test
    public void testJoinGroup() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"ConsumerId\": \"test_sub_id-1\",\"VersionId\":1,\"SessionTimeout\": 60000}")
        );
        JoinGroupResult joinGroupResult = client.joinGroup("test_project", "test_topic", "test_sub_id", 60000);
        Assert.assertEquals("test_sub_id-1", joinGroupResult.getConsumerId());
        Assert.assertEquals(60000, joinGroupResult.getSessionTimeout());
        Assert.assertEquals(1, joinGroupResult.getVersionId());
        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/subscriptions/test_sub_id")
                        .withBody(exact("{\"Action\":\"joinGroup\",\"SessionTimeout\":60000}"))
        );
    }

    @Test
    public void testHeartbeat() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"ShardList\": [\"0\", \"1\"], \"TotalPlan\": \"\", \"PlanVersion\": 1}")
        );
        List<String> holdShardList = Arrays.asList("0", "1");
        HeartbeatResult heartbeatResult = client.heartbeat("test_project", "test_topic", "test_sub_id", "test_consumer_id", 1, holdShardList, null);
        Assert.assertEquals("testId", heartbeatResult.getRequestId());
        Assert.assertEquals(1, heartbeatResult.getPlanVersion());
        Assert.assertEquals("", heartbeatResult.getTotalPlan());
        Assert.assertEquals(2, heartbeatResult.getShardList().size());
        Assert.assertEquals("0", heartbeatResult.getShardList().get(0));
        Assert.assertEquals("1", heartbeatResult.getShardList().get(1));
        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/subscriptions/test_sub_id")
                        .withBody(exact("{\"Action\":\"heartbeat\",\"ConsumerId\":\"test_consumer_id\",\"VersionId\":1,\"HoldShardList\":[\"0\",\"1\"]}"))
        );
    }

    @Test
    public void testSyncGroup() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("")
        );
        List<String> releaseShardList = Arrays.asList("0", "1");
        List<String> readEndShardList = Arrays.asList("2", "3");
        SyncGroupResult syncGroupResult = client.syncGroup("test_project", "test_topic", "test_sub_id", "test_consumer_id", 1, releaseShardList, readEndShardList);
        Assert.assertEquals("testId", syncGroupResult.getRequestId());
        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/subscriptions/test_sub_id")
                        .withBody(exact("{\"Action\":\"syncGroup\",\"ConsumerId\":\"test_consumer_id\",\"VersionId\":1,\"ReleaseShardList\":[\"0\",\"1\"],\"ReadEndShardList\":[\"2\",\"3\"]}"))
        );
    }

    @Test
    public void testLeaveGroup() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("")
        );
        LeaveGroupResult leaveGroupResult = client.leaveGroup("test_project", "test_topic", "test_sub_id", "test_consumer_id", 1);
        Assert.assertEquals("testId", leaveGroupResult.getRequestId());
        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/subscriptions/test_sub_id")
                        .withBody(exact("{\"Action\":\"leaveGroup\",\"ConsumerId\":\"test_consumer_id\",\"VersionId\":1}"))
        );
    }
}
