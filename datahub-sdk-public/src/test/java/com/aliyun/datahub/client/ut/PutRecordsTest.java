package com.aliyun.datahub.client.ut;

import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.model.*;
import org.mockserver.matchers.Times;
import org.mockserver.model.Header;
import org.mockserver.verify.VerificationTimes;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class PutRecordsTest extends MockServer {

    @Test
    public void testPutBlobRecords() {
        mockServerClient.when(request(), Times.exactly(1)).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"FailedRecordCount\": 0,\"FailedRecords\": []}")
        );

        // put blob
        List<RecordEntry> recordEntries = new ArrayList<>();
        recordEntries.add(new RecordEntry() {{
            setShardId("0");
            addAttribute("key1", "value1");
            setRecordData(new BlobRecordData("AAAA".getBytes()));
        }});

        recordEntries.add(new RecordEntry() {{
            setShardId("1");
            addAttribute("key1", "value1");
            setRecordData(new BlobRecordData("BBBB".getBytes()));
        }});

        PutRecordsResult putRecordsResult = client.putRecords("test_project", "test_topic", recordEntries);
        Assert.assertEquals(0, putRecordsResult.getFailedRecordCount());
        Assert.assertNull(putRecordsResult.getFailedRecords());
        Assert.assertTrue(putRecordsResult.getPutErrorEntries().isEmpty());

        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/shards")
                        .withBody("{\"Action\":\"pub\",\"Records\":[{\"ShardId\":\"0\",\"Attributes\":{\"key1\":\"value1\"},\"Data\":\"QUFBQQ==\"},{\"ShardId\":\"1\",\"Attributes\":{\"key1\":\"value1\"},\"Data\":\"QkJCQg==\"}]}"),
                VerificationTimes.once()
        );
    }

    @Test
    public void testPutBlobRecordsWithFailedReturn() {
        mockServerClient.when(request(), Times.exactly(1)).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"FailedRecordCount\": 2,\"FailedRecords\": [{\"ErrorCode\": \"MalformedRecord\",\"ErrorMessage\": \"Record field size not match\",\"Index\": 0}," +
                        "{\"ErrorCode\": \"InvalidShardId\",\"ErrorMessage\": \"Invalid shard id\",\"Index\": 1}]}")
        );

        // put blob
        List<RecordEntry> recordEntries = new ArrayList<>();
        recordEntries.add(new RecordEntry() {{
            setShardId("0");
            addAttribute("key1", "value1");
            setRecordData(new BlobRecordData("AAAA".getBytes()));
        }});

        recordEntries.add(new RecordEntry() {{
            setShardId("1");
            addAttribute("key1", "value1");
            setRecordData(new BlobRecordData("BBBB".getBytes()));
        }});

        PutRecordsResult putRecordsResult = client.putRecords("test_project", "test_topic", recordEntries);
        Assert.assertEquals(2, putRecordsResult.getFailedRecordCount());
        Assert.assertEquals(2, putRecordsResult.getFailedRecords().size());
        Assert.assertEquals("MalformedRecord", putRecordsResult.getPutErrorEntries().get(0).getErrorcode());
        Assert.assertEquals("InvalidShardId", putRecordsResult.getPutErrorEntries().get(1).getErrorcode());
        Assert.assertEquals("Invalid shard id", putRecordsResult.getPutErrorEntries().get(1).getMessage());
        Assert.assertEquals(1, putRecordsResult.getPutErrorEntries().get(1).getIndex());
        Assert.assertEquals("0", putRecordsResult.getFailedRecords().get(0).getShardId());
        Assert.assertEquals("1", putRecordsResult.getFailedRecords().get(1).getShardId());

        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/shards")
                        .withBody("{\"Action\":\"pub\",\"Records\":[{\"ShardId\":\"0\",\"Attributes\":{\"key1\":\"value1\"},\"Data\":\"QUFBQQ==\"},{\"ShardId\":\"1\",\"Attributes\":{\"key1\":\"value1\"},\"Data\":\"QkJCQg==\"}]}"),
                VerificationTimes.once()
        );

    }

    @Test
    public void testPutTupleRecordsWithFailReturn() {
        // put tuple
        List<RecordEntry> recordEntries = new ArrayList<>();
        mockServerClient.when(request(), Times.exactly(1)).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"FailedRecordCount\": 0,\"FailedRecords\": []}")
        );

        final RecordSchema schema = new RecordSchema();
        schema.addField(new Field("field1", FieldType.STRING));
        schema.addField(new Field("field2", FieldType.BIGINT));

        recordEntries.add(new RecordEntry() {{
            setShardId("0");
            addAttribute("key1", "value1");
            setRecordData(new TupleRecordData(schema) {{ setField(0, "AAAA"); setField(1, 100);}});
        }});

        recordEntries.add(new RecordEntry() {{
            setShardId("1");
            addAttribute("key2", "value2");
            setRecordData(new TupleRecordData(schema) {{ setField(0, "BBBB"); setField(1, 101);}});
        }});
        PutRecordsResult putRecordsResult = client.putRecords("test_project", "test_topic", recordEntries);
        Assert.assertEquals(0, putRecordsResult.getFailedRecordCount());
        Assert.assertNull(putRecordsResult.getFailedRecords());
        Assert.assertTrue(putRecordsResult.getPutErrorEntries().isEmpty());

        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/shards")
                        .withBody("{\"Action\":\"pub\",\"Records\":[{\"ShardId\":\"0\",\"Attributes\":{\"key1\":\"value1\"},\"Data\":[\"AAAA\",\"100\"]},{\"ShardId\":\"1\",\"Attributes\":{\"key2\":\"value2\"},\"Data\":[\"BBBB\",\"101\"]}]}"),
                VerificationTimes.once()
        );
    }

    @Test
    public void putRecordWithNoRecords() {
        try {
            client.putRecords("test_project", "test_topic", null);
            fail("Put records with null should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }

        try {
            client.putRecords("test_project", "test_topic", new ArrayList<RecordEntry>());
            fail("Put records with empty records should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void putRecordWithNotAllowNull() {
        List<RecordEntry> recordEntries = new ArrayList<>();
        final RecordSchema schema = new RecordSchema();
        schema.addField(new Field("field1", FieldType.STRING));
        schema.addField(new Field("field2", FieldType.BIGINT, false));

        recordEntries.add(new RecordEntry() {{
            setShardId("0");
            addAttribute("key1", "value1");
            addAttribute("key2", "value2");
            setRecordData(new TupleRecordData(schema) {{ setField(0, "AAAA"); setField(1, 100);}});
        }});

        recordEntries.add(new RecordEntry() {{
            setShardId("1");
            addAttribute("key1", "value1");
            addAttribute("key2", "value2");
            setRecordData(new TupleRecordData(schema) {{ setField(0, "BBBB"); setField(1, null);}});
        }});
        client.putRecords("test_project", "test_topic", recordEntries);
    }
}
