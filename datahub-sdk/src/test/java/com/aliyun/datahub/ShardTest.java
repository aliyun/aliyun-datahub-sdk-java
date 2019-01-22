package com.aliyun.datahub;

import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.exception.*;
import com.aliyun.datahub.model.*;
import com.aliyun.datahub.util.DatahubTestUtils;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.List;
import java.util.Random;
@Test
public class ShardTest {
    private String projectName = null;
    private String topicName = null;
    private DatahubClient client = null;
    private int shardCount = 3;
    private int lifeCycle = 7;

    @BeforeClass
    public void setUp() {
        try {
            client = new DatahubClient(DatahubTestUtils.getConf());
            projectName = DatahubTestUtils.getProjectName();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public void tearDown() {
        try {
            // do nothing
        } catch (DatahubClientException e) {
            e.printStackTrace();
        }
    }

    @BeforeMethod
    public void SetUp() {
        try {
            RecordType type = RecordType.TUPLE;
            RecordSchema schema = new RecordSchema();
            schema.addField(new Field("test", FieldType.BIGINT));
            String comment = "";
            Random random = new Random(System.currentTimeMillis());
            topicName = DatahubTestUtils.getRandomTopicName();
            System.out.println("Topic Name is " + topicName);
            client.createTopic(projectName, topicName, shardCount, lifeCycle, type, schema, comment);
        } catch (DatahubClientException e) {
            e.printStackTrace();
        }
    }

    @AfterMethod
    public void TearDown() {
        try {
            client.deleteTopic(projectName, topicName);
        } catch (DatahubClientException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testListShardNormal() {
        ListShardRequest req = new ListShardRequest(projectName, topicName);
        ListShardResult resp = client.listShard(req);
        List<ShardEntry> status = resp.getShards();
        Assert.assertEquals(status.size(), shardCount);
    }

    @Test
    public void testSplitShardNormal() {
        try {
            Thread.sleep(5000);
            SplitShardResult resp = client.splitShard(projectName, topicName, "0", "2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
            Assert.assertEquals(resp.getShards().size(), 2);
            Assert.assertEquals(resp.getShards().get(0).getShardId(), "3");
            Assert.assertEquals(resp.getShards().get(0).getBeginHashKey(), "00000000000000000000000000000000");
            Assert.assertEquals(resp.getShards().get(0).getEndHashKey(), "2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
            Assert.assertEquals(resp.getShards().get(1).getShardId(), "4");
            Assert.assertEquals(resp.getShards().get(1).getBeginHashKey(), "2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
            Assert.assertEquals(resp.getShards().get(1).getEndHashKey(), "55555555555555555555555555555555");
        } catch (InterruptedException e) {
            Assert.assertTrue(false);
        }
    }

    @Test(expectedExceptions = LimitExceededException.class)
    public void testSplitShardExceedTimeLimit() {
        //Time limit 5s after create
        client.splitShard(projectName, topicName, "0");
    }

    @Test
    public void testSplitShardNormalWithoutKey() {
        try {
            Thread.sleep(5000);
            client.splitShard(projectName, topicName, "0");
        } catch (InterruptedException e) {
            Assert.assertTrue(false);
        }
    }

    @Test(expectedExceptions = InvalidOperationException.class)
    public void testSplitShardSealed() {
        try {
            Thread.sleep(5000);
            SplitShardResult resp = client.splitShard(projectName, topicName, "0");
            Thread.sleep(5000);
            client.splitShard(projectName, topicName, "0");
        } catch (InterruptedException e) {
            Assert.assertTrue(false);
        }
    }

    @Test(expectedExceptions = DatahubClientException.class)
    public void testSplitShardNotExistWithoutKey() {
        try {
            Thread.sleep(5000);
            client.splitShard(projectName, topicName, "5");
        } catch (InterruptedException e) {
            Assert.assertTrue(false);
        }
    }

    @Test(expectedExceptions = ResourceNotFoundException.class)
    public void testSplitShardNotExistWithKey() {
        try {
            Thread.sleep(5000);
            client.splitShard(projectName, topicName, "5", "2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        } catch (InterruptedException e) {
            Assert.assertTrue(false);
        }
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void testSplitInvalidKey() {
        try {
            Thread.sleep(5000);
            client.splitShard(projectName, topicName, "1", "2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        } catch (InterruptedException e) {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testMergeShardNormal() {
        try {
            Thread.sleep(5000);
            MergeShardResult resp = client.mergeShard(projectName, topicName, "0", "1");
            Assert.assertEquals(resp.getChildShard().getShardId(), "3");
            Assert.assertEquals(resp.getChildShard().getBeginHashKey(), "00000000000000000000000000000000");
            Assert.assertEquals(resp.getChildShard().getEndHashKey(), "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
            ListShardResult shards = client.listShard(projectName, topicName);
            List<ShardEntry> status = shards.getShards();
            Assert.assertEquals(status.size(), 4);
        } catch (InterruptedException e) {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testMergeShardNormalReverse() {
        try {
            Thread.sleep(5000);
            MergeShardResult resp = client.mergeShard(projectName, topicName, "1", "0");
            Assert.assertEquals(resp.getChildShard().getShardId(), "3");
            Assert.assertEquals(resp.getChildShard().getBeginHashKey(), "00000000000000000000000000000000");
            Assert.assertEquals(resp.getChildShard().getEndHashKey(), "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
            ListShardResult shards = client.listShard(projectName, topicName);
            List<ShardEntry> status = shards.getShards();
            Assert.assertEquals(status.size(), 4);
        } catch (InterruptedException e) {
            Assert.assertTrue(false);
        }
    }

    @Test(expectedExceptions = InvalidOperationException.class)
    public void testMergeShardSealed() {
        try {
            Thread.sleep(5000);
            MergeShardResult resp = client.mergeShard(projectName, topicName, "0", "1");
            Assert.assertEquals(resp.getChildShard().getShardId(), "3");
            Assert.assertEquals(resp.getChildShard().getBeginHashKey(), "00000000000000000000000000000000");
            Assert.assertEquals(resp.getChildShard().getEndHashKey(), "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
            Thread.sleep(5000);
            client.mergeShard(projectName, topicName, "1", "2");
        } catch (InterruptedException e) {
            Assert.assertTrue(false);
        }
    }

    @Test(expectedExceptions = LimitExceededException.class)
    public void testMergeShardExceedTimeLimit() {
        //Time limit 5s after create
        client.mergeShard(projectName, topicName, "0", "1");
    }

    @Test(expectedExceptions = ResourceNotFoundException.class)
    public void testMergeShardNotExist() {
        try {
            Thread.sleep(5000);
            client.mergeShard(projectName, topicName, "3", "2");
        } catch (InterruptedException e) {
            Assert.assertTrue(false);
        }
    }

    @Test(expectedExceptions = ResourceNotFoundException.class)
    public void testMergeShardNotExistAdjacent() {
        try {
            Thread.sleep(5000);
            client.mergeShard(projectName, topicName, "2", "3");
        } catch (InterruptedException e) {
            Assert.assertTrue(false);
        }
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void testMergeShardNotAdjacent() {
        try {
            Thread.sleep(5000);
            client.mergeShard(projectName, topicName, "0", "2");
            Assert.assertTrue(false);
        } catch (InterruptedException e) {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testSplitShardNormalBoundaryKey() {
        try {
            Thread.sleep(5000);
            try {
                client.splitShard(projectName, topicName, "0", "00000000000000000000000000000000");
                Assert.assertTrue(false);
            } catch (InvalidParameterException e) {
                Assert.assertEquals(e.getErrorCode(), "InvalidParameter");
            }
            try {
                client.splitShard(projectName, topicName, "0", "55555555555555555555555555555555");
                Assert.assertTrue(false);
            } catch (InvalidParameterException e) {
                Assert.assertEquals(e.getErrorCode(), "InvalidParameter");
            }
        } catch (InterruptedException e) {
            Assert.assertTrue(false);
        }
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void testMergeShardNormalBoundaryShard() {
        try {
            Thread.sleep(5000);
            client.mergeShard(projectName, topicName, "2", "2");
        } catch (InterruptedException e) {
            Assert.assertTrue(false);
        }
    }
}
