package com.aliyun.datahub.client.e2e;

import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.exception.ResourceNotFoundException;
import com.aliyun.datahub.client.model.*;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.junit.Assert.fail;

public class TopicTest extends BaseTest {

    @BeforeClass
    public static void setUpBeforeClass() {
        bCreateTopic = false;
        BaseTest.setUpBeforeClass();
    }

    @Test
    public void testTupleTopicNormal() {
        // create tuple topic
        String topicName = getTestTopicName(RecordType.TUPLE);
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("field1", FieldType.STRING));
        schema.addField(new Field("field2", FieldType.BIGINT, false));
        client.createTopic(TEST_PROJECT_NAME, topicName, 2, 1, RecordType.TUPLE, schema, "comment");
        client.waitForShardReady(TEST_PROJECT_NAME, topicName);

        // get topic
        GetTopicResult getTopicResult = client.getTopic(TEST_PROJECT_NAME, topicName);
        Assert.assertEquals(2, getTopicResult.getShardCount());
        Assert.assertEquals(1, getTopicResult.getLifeCycle());
        Assert.assertEquals(RecordType.TUPLE, getTopicResult.getRecordType());
        Assert.assertEquals("comment", getTopicResult.getComment());
        RecordSchema returnSchema = getTopicResult.getRecordSchema();
        Assert.assertEquals(2, returnSchema.getFields().size());
        Assert.assertEquals(FieldType.STRING, returnSchema.getField(0).getType());
        Assert.assertEquals(FieldType.BIGINT, returnSchema.getField("field2").getType());
        Assert.assertTrue(returnSchema.getField(0).isAllowNull());
        Assert.assertFalse(returnSchema.getField(1).isAllowNull());

        // update topic
        client.updateTopic(TEST_PROJECT_NAME, topicName, "update comment");
        getTopicResult = client.getTopic(TEST_PROJECT_NAME, topicName);
        Assert.assertEquals(1, getTopicResult.getLifeCycle());
        Assert.assertEquals("update comment", getTopicResult.getComment());

        // list topic
        ListTopicResult listTopicResult = client.listTopic(TEST_PROJECT_NAME);
        Assert.assertTrue(listTopicResult.getTopicNames().contains(topicName));

        // list shard
        ListShardResult listShardResult = client.listShard(TEST_PROJECT_NAME, topicName);
        Assert.assertEquals(2, listShardResult.getShards().size());
        ShardEntry shardEntry = listShardResult.getShards().get(0);
        if (!shardEntry.getShardId().equals("0")) {
            shardEntry = listShardResult.getShards().get(1);
        }
        Assert.assertEquals("0", shardEntry.getShardId());
        Assert.assertEquals(ShardState.ACTIVE, shardEntry.getState());
        Assert.assertEquals("1", shardEntry.getRightShardId());
        Assert.assertNull(shardEntry.getLeftShardId());
        Assert.assertEquals("00000000000000000000000000000000", shardEntry.getBeginHashKey());
        Assert.assertEquals("7FFFFFFFFFFFFFFF7FFFFFFFFFFFFFFF", shardEntry.getEndHashKey());
        shardEntry = listShardResult.getShards().get(1);
        if (!shardEntry.getShardId().equals("1")) {
            shardEntry = listShardResult.getShards().get(0);
        }
        Assert.assertEquals("0", shardEntry.getLeftShardId());
        Assert.assertNull(shardEntry.getRightShardId());

        // sleep between create and split
        try {
            Thread.sleep(6000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // split shard
        SplitShardResult splitShardResult = client.splitShard(TEST_PROJECT_NAME, topicName, "0");
        Assert.assertEquals(2, splitShardResult.getNewShards().size());
        Assert.assertEquals("2", splitShardResult.getNewShards().get(0).getShardId());
        Assert.assertEquals("3FFFFFFFFFFFFFFFBFFFFFFFFFFFFFFF", splitShardResult.getNewShards().get(0).getEndHashKey());
        Assert.assertEquals("3", splitShardResult.getNewShards().get(1).getShardId());


        // sleep between split and shard
        try {
            Thread.sleep(6000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // merge shard
        MergeShardResult mergeShardResult = client.mergeShard(TEST_PROJECT_NAME, topicName, "2", "3");
        Assert.assertEquals("4", mergeShardResult.getShardId());
        Assert.assertEquals("00000000000000000000000000000000", mergeShardResult.getBeginHashKey());
        Assert.assertEquals("7FFFFFFFFFFFFFFF7FFFFFFFFFFFFFFF", mergeShardResult.getEndHashKey());
        client.waitForShardReady(TEST_PROJECT_NAME, topicName);
        listShardResult = client.listShard(TEST_PROJECT_NAME, topicName);
        Assert.assertEquals(5, listShardResult.getShards().size());
        for (ShardEntry entry : listShardResult.getShards()) {
            String shardId = entry.getShardId();
            if (shardId.equalsIgnoreCase("1") || shardId.equalsIgnoreCase("4")) {
                Assert.assertEquals(ShardState.ACTIVE, entry.getState());
            } else {
                Assert.assertEquals(ShardState.CLOSED, entry.getState());
            }
        }
        // get meter info
        try {
            GetMeterInfoResult getMeterInfoResult = client.getMeterInfo(TEST_PROJECT_NAME, topicName, "0");
            fail("Get meter with no data not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ResourceNotFoundException);
        }

        // append field
        client.appendField(TEST_PROJECT_NAME, topicName, new Field("field3", FieldType.BOOLEAN));
        getTopicResult = client.getTopic(TEST_PROJECT_NAME, topicName);
        returnSchema = getTopicResult.getRecordSchema();
        Assert.assertEquals(3, returnSchema.getFields().size());
        Assert.assertEquals(FieldType.BOOLEAN, returnSchema.getField("field3").getType());

        client.deleteTopic(TEST_PROJECT_NAME, topicName);
    }

    @Test
    public void testCreateBolbTopicNormal() {
        String topicName = getTestTopicName(RecordType.BLOB);
        client.createTopic(TEST_PROJECT_NAME, topicName, 1, 1, RecordType.BLOB, "comment");
        GetTopicResult getTopicResult = client.getTopic(TEST_PROJECT_NAME, topicName);
        Assert.assertEquals(1, getTopicResult.getShardCount());
        Assert.assertEquals(1, getTopicResult.getLifeCycle());
        Assert.assertEquals(RecordType.BLOB, getTopicResult.getRecordType());

        client.deleteTopic(TEST_PROJECT_NAME, topicName);
    }

    @Test
    public void testGetUnknownProject() {
        try {
            client.getTopic(TEST_PROJECT_NAME, "UnknownProject");
            fail("Get project with unknown not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ResourceNotFoundException);
        }
    }

    @Test
    public void testAppendNotNullField() {
        // create tuple topic
        String topicName = getTestTopicName(RecordType.TUPLE);
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("field1", FieldType.STRING));
        schema.addField(new Field("field2", FieldType.BIGINT, false));
        client.createTopic(TEST_PROJECT_NAME, topicName, 2, 1, RecordType.TUPLE, schema, "comment");

        try {
            client.appendField(TEST_PROJECT_NAME, topicName, new Field("field3", FieldType.BOOLEAN, false));
            fail("Append not null field not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
        client.deleteTopic(TEST_PROJECT_NAME, topicName);
    }


    @Test
    public void testCreateTopicWithWrongProject() {
        String topicName = getTestTopicName(RecordType.TUPLE);
        try {
            client.createTopic("UnknownProject", topicName, 1, 1, RecordType.BLOB,"comment");
            fail("Create topic with wrong project not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ResourceNotFoundException);
        }
    }

    @Test
    public void testDeleteUnknownTopic() {
        String topicName = getTestTopicName(RecordType.TUPLE);
        try {
            client.deleteTopic(TEST_PROJECT_NAME, "UnknownTopic");
            fail("DeleteUnknownTopic should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ResourceNotFoundException);
        }
    }
}