package com.aliyun.datahub.oldsdk;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.InvalidParameterException;
import com.aliyun.datahub.exception.ResourceNotFoundException;
import com.aliyun.datahub.model.GetTopicResult;
import com.aliyun.datahub.model.ListTopicResult;
import com.aliyun.datahub.oldsdk.util.DatahubTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Random;

public class TopicTest {
    private String projectName = null;
    private String topicName = null;
    private DatahubClient client = null;

    @BeforeClass
    public void setUp() {
        try {
            client = new DatahubClient(DatahubTestUtils.getConf());
            projectName = DatahubTestUtils.getProjectName();
            //client.createProject(projectName, "This is unit test project");
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

    @Test
    public void testCreateTopicNormal() {
        int shardCount = 3;
        int lifeCycle = 7;
        RecordType type = RecordType.TUPLE;
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.BIGINT));
        String comment = "";
        topicName = DatahubTestUtils.getRandomTopicName();
        try {
            client.createTopic(projectName, topicName, shardCount, lifeCycle, type, schema, comment);
            Assert.assertTrue(true);
        } catch(DatahubClientException e) {
            Assert.assertTrue(false);
        } finally {
            client.deleteTopic(projectName, topicName);
        }
    }

    @Test
    public void testCreateBlobTopicNormal() throws Exception{
        int shardCount = 3;
        int lifeCycle = 7;
        RecordType type = RecordType.BLOB;
        String comment = "";
        topicName = DatahubTestUtils.getRandomTopicName();

        client.createTopic(projectName, topicName, shardCount, lifeCycle, type, comment);
        client.deleteTopic(projectName, topicName);
    }

    @Test
    public void testGetBlobTopicNormal() throws Exception{
        int shardCount = 3;
        int lifeCycle = 7;
        RecordType type = RecordType.BLOB;
        String comment = "";
        topicName = DatahubTestUtils.getRandomTopicName();
        client.createTopic(projectName, topicName, shardCount, lifeCycle, type, comment);
        GetTopicResult getTopicResult = client.getTopic(projectName, topicName);
        Assert.assertEquals(getTopicResult.getRecordType(), RecordType.BLOB);

        client.deleteTopic(projectName, topicName);
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void testCreateBlobTopicWithSchema() throws Exception{
        int shardCount = 3;
        int lifeCycle = 7;
        RecordType type = RecordType.BLOB;
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.BIGINT));
        String comment = "";
        topicName = DatahubTestUtils.getRandomTopicName();
        client.createTopic(projectName, topicName, shardCount, lifeCycle, type, schema, comment);
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void testCreateTupleTopicWithNoProject() {
        final int shardCount = 3;
        final int lifeCycle = 7;
        final RecordType type = RecordType.TUPLE;
        final String comment = "";
        client.createTopic(null, topicName, shardCount, lifeCycle, type, comment);
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void testCreateTopicWithNoProject() {
        final int shardCount = 3;
        final int lifeCycle = 7;
        final RecordType type = RecordType.TUPLE;
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.BIGINT));
        final String comment = "";
        client.createTopic(null, topicName, shardCount, lifeCycle, type, schema, comment);
    }

    @Test(expectedExceptions = ResourceNotFoundException.class)
    public void testCreateTopicWithWrongProject() {
        final String wrongProjectName = "xxx";
        final int shardCount = 3;
        final int lifeCycle = 7;
        final RecordType type = RecordType.TUPLE;
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.BIGINT));
        final String comment = "";
        topicName = DatahubTestUtils.getRandomTopicName();
        client.createTopic(wrongProjectName, topicName, shardCount, lifeCycle, type, schema, comment);
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void testCreateTopicWithNoTopic() {
        final int shardCount = 3;
        final int lifeCycle = 7;
        final RecordType type = RecordType.TUPLE;
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.BIGINT));
        final String comment = "";
        client.createTopic(projectName, null, shardCount, lifeCycle, type, schema, comment);
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void testCreateTopicWithNoSchema() {
        int shardCount = 3;
        int lifeCycle = 7;
        RecordType type = RecordType.TUPLE;
        String comment = "";
        topicName = DatahubTestUtils.getRandomTopicName();
        client.createTopic(projectName, topicName, shardCount, lifeCycle, type, null, comment);
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void testCreateTopicWithNoType() {
        int shardCount = 3;
        int lifeCycle = 7;
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.BIGINT));
        String comment = "";
        topicName = DatahubTestUtils.getRandomTopicName();
        client.createTopic(projectName, topicName, shardCount, lifeCycle, null, schema, comment);
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void testCreateTopicWithNoComment() {
        int shardCount = 3;
        int lifeCycle = 7;
        RecordType type = RecordType.TUPLE;
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.BIGINT));
        topicName = DatahubTestUtils.getRandomTopicName();
        client.createTopic(projectName, topicName, shardCount, lifeCycle, type, schema, null);
    }

    @Test
    public void testCreateTopicLifeCycle() {
        int shardCount = 3;
        RecordType type = RecordType.TUPLE;
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.BIGINT));
        topicName = DatahubTestUtils.getRandomTopicName();
        try {
            client.createTopic(projectName, topicName, shardCount, -1, type, schema, null);
            Assert.assertTrue(false);
        } catch (DatahubClientException e) {
            Assert.assertTrue(true);
        }

        topicName = DatahubTestUtils.getRandomTopicName();
        try {
            client.createTopic(projectName, topicName, shardCount, 0, type, schema, null);
            Assert.assertTrue(false);
        } catch (DatahubClientException e) {
            Assert.assertTrue(true);
        }

        topicName = DatahubTestUtils.getRandomTopicName();
        try {
            client.createTopic(projectName, topicName, shardCount, 8, type, schema, null);
            Assert.assertTrue(false);
        } catch (DatahubClientException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testDeleteTopicNormal() {
        int shardCount = 3;
        int lifeCycle = 7;
        RecordType type = RecordType.TUPLE;
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.BIGINT));
        String comment = "";
        topicName = DatahubTestUtils.getRandomTopicName();
        try {
            client.createTopic(projectName, topicName, shardCount, lifeCycle, type, schema, comment);
        } catch (DatahubClientException e) {
            Assert.assertTrue(false);
        } finally {
            client.deleteTopic(projectName, topicName);
            Assert.assertTrue(true);
        }
    }

    @Test(expectedExceptions = DatahubClientException.class)
    public void testDeleteTopicWithNoCreate() {
        topicName = DatahubTestUtils.getRandomTopicName();
        client.deleteTopic(projectName, topicName);
    }

    @Test
    public void testUpdateTopicNormal() {
        int shardCount = 3;
        int lifeCycle = 7;
        RecordType type = RecordType.TUPLE;
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.BIGINT));
        String comment = "create";
        topicName = DatahubTestUtils.getRandomTopicName();
        client.createTopic(projectName, topicName, shardCount, lifeCycle, type, schema, comment);
        lifeCycle = 5;
        comment = "update";
        client.updateTopic(projectName, topicName, lifeCycle, comment);
        client.deleteTopic(projectName, topicName);
    }

    @Test
    public void testListTopic() {
        ListTopicResult rs = client.listTopic(projectName);
        System.out.print(rs.getTopics());
    }

    @Test
    public void testGetTopic() {
        int shardCount = 3;
        int lifeCycle = 7;
        RecordType type = RecordType.TUPLE;
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.BIGINT));
        String comment = "create";
        topicName = DatahubTestUtils.getRandomTopicName();
        client.createTopic(projectName, topicName, shardCount, lifeCycle, type, schema, comment);
        GetTopicResult topic = client.getTopic(projectName, topicName);
        Assert.assertEquals(topic.getTopicName(), topicName);
        Assert.assertEquals(topic.getShardCount(), shardCount);
        Assert.assertEquals(topic.getLifeCycle(), lifeCycle);
        Assert.assertEquals(topic.getComment(), comment);
        Assert.assertEquals(topic.getRecordSchema().toJsonString(), schema.toJsonString());
        Assert.assertEquals(topic.getRecordType(), type);

        lifeCycle = 5;
        comment = "upate";
        client.updateTopic(projectName, topicName, lifeCycle, comment);
        topic = client.getTopic(projectName, topicName);
        Assert.assertEquals(topic.getTopicName(), topicName);
        Assert.assertEquals(topic.getShardCount(), shardCount);
//        Assert.assertEquals(topic.getLifeCycle(), lifeCycle);
        Assert.assertEquals(topic.getComment(), comment);
        Assert.assertEquals(topic.getRecordSchema().toJsonString(), schema.toJsonString());
        Assert.assertEquals(topic.getRecordType(), type);

        client.deleteTopic(projectName, topicName);
    }

    @Test
    public void testGetTopicNotNull() {
        int shardCount = 3;
        int lifeCycle = 7;
        RecordType type = RecordType.TUPLE;
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.BIGINT));
        schema.addField(new Field("fnull", FieldType.STRING, true));
        String comment = "create";
        topicName = DatahubTestUtils.getRandomTopicName();
        client.createTopic(projectName, topicName, shardCount, lifeCycle, type, schema, comment);
        GetTopicResult topic = client.getTopic(projectName, topicName);
        Assert.assertEquals(topic.getTopicName(), topicName);
        Assert.assertEquals(topic.getShardCount(), shardCount);
        Assert.assertEquals(topic.getLifeCycle(), lifeCycle);
        Assert.assertEquals(topic.getComment(), comment);
        Assert.assertEquals(topic.getRecordSchema().toJsonString(), schema.toJsonString());
        Assert.assertEquals(topic.getRecordType(), type);

        client.deleteTopic(projectName, topicName);
    }

    @Test
    public void testCreateTopicUpperCase() {
        int shardCount = 3;
        int lifeCycle = 7;
        RecordType type = RecordType.TUPLE;
        RecordSchema upperSchema = new RecordSchema();
        upperSchema.addField(new Field("TestBigint1", FieldType.BIGINT));
        upperSchema.addField(new Field("TestDouble2", FieldType.DOUBLE));
        String comment = "create";
        Random random = new Random(System.currentTimeMillis());
        topicName = String.format("ut_test_topic_%s", Long.toHexString(random.nextLong()));
        client.createTopic(projectName, topicName, shardCount, lifeCycle, type, upperSchema, comment);
        GetTopicResult topic = client.getTopic(projectName, topicName);

        RecordSchema lowerSchema = new RecordSchema();
        lowerSchema.addField(new Field("testbigint1", FieldType.BIGINT));
        lowerSchema.addField(new Field("testdouble2", FieldType.DOUBLE));
        Assert.assertEquals(topic.getTopicName(), topicName);
        Assert.assertEquals(topic.getShardCount(), shardCount);
        Assert.assertEquals(topic.getLifeCycle(), lifeCycle);
        Assert.assertEquals(topic.getComment(), comment);
        Assert.assertEquals(topic.getRecordSchema().toJsonString(), lowerSchema.toJsonString());
        Assert.assertEquals(topic.getRecordType(), type);

        client.deleteTopic(projectName, topicName);

    }
}
