package com.aliyun.datahub.client.e2e;

import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.exception.LimitExceededException;
import com.aliyun.datahub.client.exception.ResourceNotFoundException;
import com.aliyun.datahub.client.exception.ShardSealedException;
import com.aliyun.datahub.client.model.MergeShardResult;
import com.aliyun.datahub.client.model.SplitShardResult;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.junit.Assert.fail;

public class ShardTest extends BaseTest {

    @BeforeClass
    public static void setUpBeforeClass() {
        bCreateTopic = true;
        BaseTest.setUpBeforeClass();
    }

    @Test
    public void testSplitShardWithSpecifiedKey() {
        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            //
        }
        SplitShardResult splitShardResult = client.splitShard(TEST_PROJECT_NAME, blobTopicName, "0", "2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        Assert.assertEquals("00000000000000000000000000000000", splitShardResult.getNewShards().get(0).getBeginHashKey());
        Assert.assertEquals("2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", splitShardResult.getNewShards().get(0).getEndHashKey());
        Assert.assertEquals("2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", splitShardResult.getNewShards().get(1).getBeginHashKey());
        Assert.assertEquals("55555555555555555555555555555555", splitShardResult.getNewShards().get(1).getEndHashKey());


        // test shard with not active key
        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            //
        }

        try {
            splitShardResult = client.splitShard(TEST_PROJECT_NAME, blobTopicName, "0", "1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
            fail("Split shard with not active shard should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ShardSealedException);
        }
    }


    @Test
    public void testSplitShardWithLimitExceed() {
        try {
            SplitShardResult splitShardResult = client.splitShard(TEST_PROJECT_NAME, blobTopicName, "0", "2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
            fail("Split shard after create immediately should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof LimitExceededException);
        }
    }

    @Test
    public void testSplitShardWithNotExistShardId() {
        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            //
        }

        try {
            SplitShardResult splitShardResult = client.splitShard(TEST_PROJECT_NAME, blobTopicName, "3", "2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
            fail("Split shard with not exist key should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ResourceNotFoundException);
        }
    }

    @Test
    public void testSplitShardWithInvalidKey() {
        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            //
        }

        try {
            SplitShardResult splitShardResult = client.splitShard(TEST_PROJECT_NAME, blobTopicName, "0", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
            fail("Split shard with not exist key should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testMergeShardWithSealed() {
        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            //
        }
        MergeShardResult mergeShardResult = client.mergeShard(TEST_PROJECT_NAME, blobTopicName, "0", "1");
        // second merge
        try {
            client.mergeShard(TEST_PROJECT_NAME, blobTopicName, "0", "1");
            fail("Merge shard with sealed shards should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ShardSealedException);
        }
    }

    @Test
    public void testMergeShardNotAdjacent() {
        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            //
        }

        try {
            client.mergeShard(TEST_PROJECT_NAME, blobTopicName, "0", "2");
            fail("Merge shard with not adjacent shards should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testMergeShardAdjacentNotExist() {
        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            //
        }

        try {
            client.mergeShard(TEST_PROJECT_NAME, blobTopicName, "0", "3");
            fail("Merge shard with adjacent not exist should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ResourceNotFoundException);
        }
    }

    @Test
    public void testMergeShardWithItself() {
        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            //
        }

        try {
            client.mergeShard(TEST_PROJECT_NAME, blobTopicName, "0", "0");
            fail("Merge shard with itself should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }
}
