package com.aliyun.datahub.client.ut;

import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.exception.NoPermissionException;
import com.aliyun.datahub.client.model.*;
import org.mockserver.matchers.Times;
import org.mockserver.model.Header;
import org.mockserver.verify.VerificationTimes;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

import static org.junit.Assert.fail;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class ShardTest extends MockServer {
    @Test
    public void testListShard() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"Shards\":[{\"ShardId\":\"0\",\"State\":\"ACTIVE\",\"ClosedTime\":100,\"BeginHashKey\":\"00000000000000000000000000000000\", \"EndHashKey\":\"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\",\"ParentShardIds\":[],\"LeftShardId\":\"0\",\"RightShardId\":\"4294967295\"}," +
                        "{\"ShardId\":\"1\", \"State\":\"CLOSED\",\"ClosedTime\":100, \"BeginHashKey\":\"00000000000000000000000000000000\", \"EndHashKey\":\"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\", \"LeftShardId\":\"0\",\"RightShardId\":\"1\"}]}")
        );
        ListShardResult listShardResult = client.listShard("test_project", "test_topic");
        List<ShardEntry> shardEntries = listShardResult.getShards();
        Assert.assertEquals(2, shardEntries.size());
        Assert.assertEquals(ShardState.ACTIVE, shardEntries.get(0).getState());
        Assert.assertEquals(ShardState.CLOSED, shardEntries.get(1).getState());
        Assert.assertTrue(shardEntries.get(0).getParentShardIds().isEmpty());
        Assert.assertNull(shardEntries.get(1).getParentShardIds());
        Assert.assertNull(shardEntries.get(0).getRightShardId());
        Assert.assertEquals("1", shardEntries.get(1).getRightShardId());

        mockServerClient.verify(
                request().withMethod("GET").withPath("/projects/test_project/topics/test_topic/shards")
        );
    }

    @Test
    public void testWaitForShardNormal() {
        mockServerClient.when(request(), Times.exactly(3)).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"Shards\":[{\"ShardId\":\"0\",\"State\":\"OPENING\",\"ClosedTime\":100,\"BeginHashKey\":\"00000000000000000000000000000000\", \"EndHashKey\":\"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\",\"ParentShardIds\":[],\"LeftShardId\":\"0\",\"RightShardId\":\"4294967295\"}]}")
        );

        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"Shards\":[{\"ShardId\":\"0\",\"State\":\"ACTIVE\",\"ClosedTime\":100,\"BeginHashKey\":\"00000000000000000000000000000000\", \"EndHashKey\":\"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\",\"ParentShardIds\":[],\"LeftShardId\":\"0\",\"RightShardId\":\"4294967295\"}]}")
        );

        client.waitForShardReady("test_project", "test_topic", 10000);
        mockServerClient.verify(
                request().withMethod("GET").withPath("/projects/test_project/topics/test_topic/shards"),
                VerificationTimes.atLeast(3)
        );
    }

    @Test
    public void testWaitForShardTimeout() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"Shards\":[{\"ShardId\":\"0\",\"State\":\"OPENING\",\"ClosedTime\":100,\"BeginHashKey\":\"00000000000000000000000000000000\", \"EndHashKey\":\"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\",\"ParentShardIds\":[],\"LeftShardId\":\"0\",\"RightShardId\":\"4294967295\"}]}")
        );

        try {
            client.waitForShardReady("test_project", "test_topic", 3000);
            fail("WaitForShardReady should throw exception because of timeout");
        } catch (DatahubClientException e) {
            Assert.assertTrue(e.getMessage().contains("timeout"));
        }
    }

    @Test
    public void testSplitShardWithClosedShard() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"Shards\":[{\"ShardId\":\"0\",\"State\":\"CLOSED\",\"BeginHashKey\":\"00000000000000000000000000000000\", \"EndHashKey\":\"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\"}]}") // here json not standard
        );

        try {
            SplitShardResult splitShardResult = client.splitShard("test_project", "test_topic", "0");
            fail("Split closed shard should throw exception");
        } catch (DatahubClientException e) {
            Assert.assertTrue(e instanceof NoPermissionException);
            Assert.assertTrue(e.getMessage().contains("active shard"));
        }
    }

    @Test
    public void testSplitShardWithInvalidHashKey() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"Shards\":[{\"ShardId\":\"0\",\"State\":\"ACTIVE\",\"BeginHashKey\":\"FFF\", \"EndHashKey\":\"000\"}]}") // here json not standard
        );

        try {
            SplitShardResult splitShardResult = client.splitShard("test_project", "test_topic", "0");
            fail("Split with invalid hash key shoud throw exception");
        } catch (DatahubClientException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid"));
        }
    }

    @Test
    public void testSplitShardWithInvalidShardId() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"Shards\":[{\"ShardId\":\"0\",\"State\":\"ACTIVE\",\"BeginHashKey\":\"FFF\", \"EndHashKey\":\"000\"}]}") // here json not standard
        );

        try {
            SplitShardResult splitShardResult = client.splitShard("test_project", "test_topic", "1");
            fail("Split with invalid shardId shoud throw exception");
        } catch (DatahubClientException e) {
            Assert.assertTrue(e.getMessage().contains("Shard not exist"));
        }
    }

    @Test
    public void testSplitWithInvalidShardIdFormat() {
        try {
            client.splitShard("test_project", "test_topic", "aa");
            fail("Split with invalid shardId format should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testSplitShard() {
        mockServerClient.when(request(), Times.exactly(1)).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"Shards\":[{\"ShardId\":\"0\",\"State\":\"ACTIVE\",\"BeginHashKey\":\"00000000000000000000000000000000\", \"EndHashKey\":\"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\"}]}")
        );

        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"NewShards\": [{\"BeginHashKey\": \"000\",\"EndHashKey\": \"7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\",\"ShardId\": \"1\"}, {\"BeginHashKey\": \"7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\",\"EndHashKey\": \"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\",\"ShardId\": \"2\"}]}")
        );

        SplitShardResult splitShardResult = client.splitShard("test_project", "test_topic", "0");
        List<ShardEntry> newShards = splitShardResult.getNewShards();
        Assert.assertEquals(2, newShards.size());
        Assert.assertEquals("1", newShards.get(0).getShardId());
        Assert.assertEquals("7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", newShards.get(1).getBeginHashKey());
        Assert.assertEquals("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", newShards.get(1).getEndHashKey());

        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/shards")
                        .withBody(exact("{\"Action\":\"split\",\"ShardId\":\"0\",\"SplitKey\":\"7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\"}"))
        );
    }

    @Test
    public void testMergeShard() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"BeginHashKey\": \"000\",\"EndHashKey\": \"FFF\",\"ShardId\": \"2\"}")
        );
        MergeShardResult mergeShardResult = client.mergeShard("test_project", "test_topic", "0", "1");
        Assert.assertEquals("2", mergeShardResult.getShardId());

        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/shards")
                        .withBody(exact("{\"Action\":\"merge\",\"ShardId\":\"0\",\"AdjacentShardId\":\"1\"}"))
        );
    }

    @Test
    public void testGetCursor() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"Cursor\": \"30005af19b3800000000000000000000\",\"RecordTime\": 1525783352873,\"Sequence\": 1}")
        );

        GetCursorResult getCursorResult = client.getCursor("test_project", "test_topic", "0", CursorType.OLDEST);
        Assert.assertEquals("30005af19b3800000000000000000000", getCursorResult.getCursor());
        Assert.assertEquals(1525783352873L, getCursorResult.getTimestamp());
        Assert.assertEquals(1, getCursorResult.getSequence());

        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/shards/0")
                        .withBody(exact("{\"Action\":\"cursor\",\"Type\":\"OLDEST\"}")),
                VerificationTimes.once()
        );

        // get cursor by sequence
        getCursorResult = client.getCursor("test_project", "test_topic", "0", CursorType.SEQUENCE, 10);
        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/shards/0")
                        .withBody(exact("{\"Action\":\"cursor\",\"Type\":\"SEQUENCE\",\"Sequence\":10}")),
                VerificationTimes.once()
        );
    }

    @Test
    public void testGetCursorWithNullType() {
        try {
            client.getCursor("test_project", "test_topic", "0", null);
            fail("Get cursor with null type should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }

        try {
            client.getCursor("test_project", "test_topic", "0", null, 10);
            fail("Get cursor with null type should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testGetCursorWithInvalidParameter() {
        try {
            client.getCursor("test_project", "test_topic", "0", CursorType.SYSTEM_TIME, -1);
            fail("Get cursor with invalid parameter should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }

        try {
            client.getCursor("test_project", "test_topic", "0", CursorType.SEQUENCE, -1);
            fail("Get cursor with invalid parameter should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testGetMeterInfo() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"ActiveTime\": 10,\"Storage\": 10}")
        );

        GetMeterInfoResult getMeteringInfoResult = client.getMeterInfo("test_project", "test_topic", "0");
        Assert.assertEquals(10, getMeteringInfoResult.getActiveTime());
        Assert.assertEquals(10, getMeteringInfoResult.getStorage());

        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/shards/0")
                        .withBody(exact("{\"Action\":\"meter\"}"))
        );
    }

    @Test
    public void testGetMeterInfoWithInvalidShardIdFormat() {
        try {
            client.getCursor("test_project", "test_topic", "test_shardId", CursorType.SYSTEM_TIME, -1);
            fail("Get cursor with invalid parameter should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }
}
