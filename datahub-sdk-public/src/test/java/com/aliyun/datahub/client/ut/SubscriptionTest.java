package com.aliyun.datahub.client.ut;

import com.aliyun.datahub.client.model.*;
import org.mockserver.model.Header;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class SubscriptionTest extends MockServer {
    @Test
    public void testCreateSubscription() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"SubId\": \"1525835229905vJHtz\"}")
        );
        CreateSubscriptionResult createSubscriptionResult = client.createSubscription("test_project", "test_topic", "test subscription");
        Assert.assertEquals("1525835229905vJHtz", createSubscriptionResult.getSubId());
        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/subscriptions")
                        .withBody(exact("{\"Action\":\"create\",\"Comment\":\"test subscription\"}"))
        );
    }

    @Test
    public void testGetSubscription() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"Comment\":\"testsubscription\",\"CreateTime\":1525835229,\"IsOwner\":true,\"LastModifyTime\":1525835229,\"State\":1,\"SubId\":\"test_subId\",\"TopicName\":\"sdk_example_topic_tuple\",\"Type\":0}")
        );

        GetSubscriptionResult getSubscriptionResult = client.getSubscription("test_project", "test_topic", "test_subId");
        Assert.assertEquals("test_subId", getSubscriptionResult.getSubId());
        Assert.assertEquals(SubscriptionState.ONLINE, getSubscriptionResult.getState());

        mockServerClient.verify(
                request().withMethod("GET").withPath("/projects/test_project/topics/test_topic/subscriptions/test_subId")
        );
    }

    @Test
    public void testUpdateSubscription() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );

        client.updateSubscription("test_project", "test_topic", "test_subId", "update comment");
        mockServerClient.verify(
                request().withMethod("PUT").withPath("/projects/test_project/topics/test_topic/subscriptions/test_subId")
                        .withBody(exact("{\"Comment\":\"update comment\"}"))
        );
    }

    @Test
    public void testListSubscription() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"TotalCount\": 3,\"Subscriptions\":[{\"Comment\":\"testsubscription\",\"CreateTime\":1525835229,\"IsOwner\":true,\"LastModifyTime\":1525835229,\"State\":1,\"SubId\":\"test_subId\",\"TopicName\":\"test_topic\",\"Type\":0}]}")
        );

        ListSubscriptionResult listSubscriptionResult = client.listSubscription("test_project", "test_topic", 1, 1);
        Assert.assertEquals(1, listSubscriptionResult.getSubscriptions().size());
        Assert.assertEquals(3, listSubscriptionResult.getTotalCount());

        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/subscriptions")
                        .withBody(exact("{\"Action\":\"list\",\"PageIndex\":1,\"PageSize\":1}"))
        );
    }

    @Test
    public void testDeleteSubscription() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );

        client.deleteSubscription("test_project", "test_topic", "test_subId");
        mockServerClient.verify(
                request().withMethod("DELETE").withPath("/projects/test_project/topics/test_topic/subscriptions/test_subId")
        );
    }

    @Test
    public void testOpenSubscriptionSession() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"Offsets\":{\"0\":{\"Sequence\":1,\"SessionId\":1,\"Timestamp\":1,\"Version\":1}}}")
        );

        List<String> shardIds = new ArrayList<String>() {{ add("0"); }};
        OpenSubscriptionSessionResult openSubscriptionSessionResult = client.openSubscriptionSession("test_project", "test_topic", "test_subId", shardIds);
        SubscriptionOffset offset = openSubscriptionSessionResult.getOffsets().get("0");
        Assert.assertEquals(1, offset.getSequence());
        Assert.assertEquals("1", offset.getSessionId());
        Assert.assertEquals(1, offset.getVersionId());

        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/subscriptions/test_subId/offsets")
                        .withBody(exact("{\"Action\":\"open\",\"ShardIds\":[\"0\"]}"))
        );
    }

    @Test
    public void testGetSubscriptionOffset() {

        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"Offsets\":{\"0\":{\"Sequence\":1,\"Timestamp\":1,\"Version\":1}}}")
        );

        List<String> shardIds = new ArrayList<String>() {{ add("0"); }};
        GetSubscriptionOffsetResult getSubscriptionOffsetResult = client.getSubscriptionOffset("test_project", "test_topic", "test_subId", shardIds);
        SubscriptionOffset offset =getSubscriptionOffsetResult.getOffsets().get("0");
        Assert.assertEquals(1, offset.getSequence());
        Assert.assertNull(offset.getSessionId());
        Assert.assertEquals(1, offset.getVersionId());

        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/subscriptions/test_subId/offsets")
                        .withBody(exact("{\"Action\":\"get\",\"ShardIds\":[\"0\"]}"))
        );

    }

    @Test
    public void testCommitSubscriptionOffset() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );

        Map<String, SubscriptionOffset> offsetMap = new HashMap<>();
        offsetMap.put("0", new SubscriptionOffset() {{
            setTimestamp(100);
            setSequence(1);
            setSessionId("1");
        }});

        client.commitSubscriptionOffset("test_project", "test_topic", "test_subId", offsetMap);

        mockServerClient.verify(
                request().withMethod("PUT").withPath("/projects/test_project/topics/test_topic/subscriptions/test_subId/offsets")
                        .withBody(exact("{\"Action\":\"commit\",\"Offsets\":{\"0\":{\"Timestamp\":100,\"Sequence\":1,\"Version\":0,\"SessionId\":1}}}"))
        );
    }

    @Test
    public void testResetSubscriptionOffsets() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );

        Map<String, SubscriptionOffset> offsetMap = new HashMap<>();
        offsetMap.put("0", new SubscriptionOffset() {{
            setTimestamp(100);
            setSequence(1);
        }});

        client.resetSubscriptionOffset("test_project", "test_topic", "test_subId", offsetMap);

        mockServerClient.verify(
                request().withMethod("PUT").withPath("/projects/test_project/topics/test_topic/subscriptions/test_subId/offsets")
                        .withBody(exact("{\"Action\":\"reset\",\"Offsets\":{\"0\":{\"Timestamp\":100,\"Sequence\":1}}}"))
        );
    }
}
