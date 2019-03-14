package com.aliyun.datahub.client.ut;

import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.model.*;
import org.mockserver.model.Header;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.junit.Assert.fail;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class TopicTest extends MockServer {

    @Test
    public void testCreateTupleTopic() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );
        client.createTopic("test_project", "test_topic", 1, 1, RecordType.BLOB, "test comment");
        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic")
                        .withBody(exact("{\"Action\":\"create\",\"ShardCount\":1,\"Lifecycle\":1,\"RecordType\":\"BLOB\",\"Comment\":\"test comment\"}"))
        );
    }

    @Test
    public void testCreateBlobTopic() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("field1", FieldType.STRING));
        schema.addField(new Field("field2", FieldType.BIGINT));
        client.createTopic("test_project", "test_topic", 1, 1, RecordType.TUPLE, schema, "test comment");
        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic")
                        .withBody(exact("{\"Action\":\"create\",\"ShardCount\":1,\"Lifecycle\":1,\"RecordType\":\"TUPLE\",\"RecordSchema\":\"{\\\"fields\\\":[{\\\"name\\\":\\\"field1\\\",\\\"type\\\":\\\"STRING\\\"},{\\\"name\\\":\\\"field2\\\",\\\"type\\\":\\\"BIGINT\\\"}]}\",\"Comment\":\"test comment\"}"))
        );
    }

    @Test
    public void testCreateTopicWithNull() {
        try {
            client.createTopic("test_project", null, 1, 1, RecordType.BLOB, "comment");
            fail("Create topic with null not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }

        try {
            client.createTopic("test_project", "test_topic", 1, 1, RecordType.BLOB, null);
            fail("Create topic with null not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }

        try {
            client.createTopic(null, "test_topic", 1, 1, RecordType.BLOB, "comment");
            fail("Create topic with null not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }

        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("field1", FieldType.STRING));
        try {
            client.createTopic(null, "test_topic", 1, 1, null, schema, "comment");
            fail("Create topic with null not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }

        try {
            client.createTopic("test_project", "test_topic", 1, 1, RecordType.TUPLE, null,"comment");
            fail("Create tuple with no schema not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testCreateTopicWithInvalid() {
        try {
            client.createTopic("test_project", "", 1, 1, RecordType.BLOB, "comment");
            fail("Create topic with empty name not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }

        try {
            client.createTopic("test_project", "test_topic", 0, 1, RecordType.BLOB, "comment");
            fail("Create project with zero shard count not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }

        try {
            client.createTopic("test_project", "test_topic", 2, -1, RecordType.BLOB, "comment");
            fail("Create project with negative lifeCycle not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }

        try {
            client.createTopic("test_project", "11aabbb", 1, 1, RecordType.BLOB, "comment");
            fail("Create project with invalid name not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }

        try {
            client.createTopic("test_project", "" /* min length */, 1, 1, RecordType.BLOB, "comment");
            fail("Create project with invalid name not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testCreateTupleWithBlobMethod() {
        try {
            client.createTopic("test_project", "test_topic", 1, 1, RecordType.TUPLE, "comment");
            fail("Create tuple with blob method not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testGetTupleTopic() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"Comment\": \"test topic tuple\",\"CreateTime\": 1525763481,\"LastModifyTime\": 1525763481,\"Lifecycle\": 1,\"RecordSchema\": \"{\\\"fields\\\":[{\\\"name\\\":\\\"field1\\\",\\\"type\\\":\\\"STRING\\\"},{\\\"name\\\":\\\"field2\\\",\\\"type\\\":\\\"BIGINT\\\"}]}\",\"RecordType\": \"TUPLE\",\"ShardCount\": 4}")
        );
        GetTopicResult getTopicResult = client.getTopic("test_project", "test_topic");
        Assert.assertEquals("test_project", getTopicResult.getProjectName());
        Assert.assertEquals("test_topic", getTopicResult.getTopicName());
        Assert.assertEquals(4, getTopicResult.getShardCount());
        Assert.assertEquals(1, getTopicResult.getLifeCycle());
        Assert.assertEquals(2, getTopicResult.getRecordSchema().getFields().size());
        Assert.assertEquals("field1", getTopicResult.getRecordSchema().getField(0).getName());
        Assert.assertEquals(FieldType.BIGINT, getTopicResult.getRecordSchema().getField(1).getType());
        Assert.assertTrue(getTopicResult.getRecordSchema().getField("field1").isAllowNull());

        mockServerClient.verify(
                request().withMethod("GET").withPath("/projects/test_project/topics/test_topic")
        );
    }

    @Test
    public void testGetBlobTopic() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"Comment\": \"test topic tuple\",\"CreateTime\": 1525763481,\"LastModifyTime\": 1525763481,\"Lifecycle\": 1,\"RecordSchema\": \"\",\"RecordType\": \"TUPLE\",\"ShardCount\": 4}")
        );
        GetTopicResult getTopicResult = client.getTopic("test_project", "test_topic");
        Assert.assertEquals("test_project", getTopicResult.getProjectName());
        Assert.assertEquals("test_topic", getTopicResult.getTopicName());
        Assert.assertEquals(4, getTopicResult.getShardCount());
        Assert.assertEquals(1, getTopicResult.getLifeCycle());
        Assert.assertNull(getTopicResult.getRecordSchema());

        mockServerClient.verify(
                request().withMethod("GET").withPath("/projects/test_project/topics/test_topic")
        );
    }

    @Test
    public void testGetTopicWithNull() {
        try {
            client.getTopic("test_project", null);
            fail("Get topic with null topic not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }

        try {
            client.getTopic(null, "test_topic");
            fail("Get topic with null project not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testListTopic() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"TopicNames\": [\"topic1\", \"topic2\"]}")
        );

        ListTopicResult listTopicResult = client.listTopic("test_project");
        Assert.assertTrue(listTopicResult.getTopicNames().contains("topic1"));
        Assert.assertTrue(listTopicResult.getTopicNames().contains("topic2"));

        mockServerClient.verify(
                request().withMethod("GET").withPath("/projects/test_project/topics")
        );
    }

    @Test
    public void testListWithNullProject() {
        try {
            client.listTopic(null);
            fail("List topic with null not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testUpdateTopic() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );

        client.updateTopic("test_project", "test_topic", "update comment");
        mockServerClient.verify(
                request().withMethod("PUT").withPath("/projects/test_project/topics/test_topic")
                        .withBody(exact("{\"Comment\":\"update comment\"}"))
        );
    }

    @Test
    public void testDeleteTopic() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );

        client.deleteTopic("test_project", "test_topic");
        mockServerClient.verify(
                request().withMethod("DELETE").withPath("/projects/test_project/topics/test_topic")
        );
    }

    @Test
    public void testAppendField() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );

        client.appendField("test_project", "test_topic", new Field("field3", FieldType.BOOLEAN));

        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic")
                        .withBody(exact("{\"Action\":\"AppendField\",\"FieldName\":\"field3\",\"FieldType\":\"BOOLEAN\"}"))
        );
    }

    @Test
    public void testAppendNotNullField() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );

        // create tuple topic
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("field1", FieldType.STRING));
        schema.addField(new Field("field2", FieldType.BIGINT, false));
        client.createTopic("test_project", "test_topic", 2, 1, RecordType.TUPLE, schema, "comment");

        try {
            client.appendField("test_project", "test_topic", new Field("field3", FieldType.BOOLEAN, false));
            fail("Append not null field not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
        client.deleteTopic("test_project", "test_topic");
    }
}
