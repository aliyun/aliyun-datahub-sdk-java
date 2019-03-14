package com.aliyun.datahub.client.ut;

import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.model.*;
import org.mockserver.matchers.Times;
import org.mockserver.model.Header;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

import static org.junit.Assert.fail;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class GetRecordsTest extends MockServer {
    @Test
    public void testGetBlobRecords() {
        // get blob
        mockServerClient.when(request(), Times.exactly(1)).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"NextCursor\":\"30005af19b3800000000000000090001\",\"RecordCount\":1,\"StartSeq\":1,\"Records\":[{\"SystemTime\":1525783352873,\"NextCursor\":\"30005af19b3800000000000000010000\",\"Cursor\":\"30005af19b3800000000000000000000\",\"Sequence\":1,\"Data\":\"QUFBQQ==\",\"Attributes\":{\"key1\":\"value1\", \"key2\":\"value2\"}}]}")
        );

        GetRecordsResult getRecordsResult = client.getRecords("test_project", "test_topic", "0", "30005af19b3800000000000000000000", 1);
        Assert.assertEquals(1, getRecordsResult.getRecordCount());
        Assert.assertEquals("30005af19b3800000000000000090001", getRecordsResult.getNextCursor());
        List<RecordEntry> recordEntities = getRecordsResult.getRecords();
        Assert.assertEquals(1, recordEntities.size());
        Assert.assertEquals(1, recordEntities.get(0).getSequence());
        Assert.assertEquals(1525783352873L, recordEntities.get(0).getSystemTime());
        Assert.assertEquals("30005af19b3800000000000000010000", recordEntities.get(0).getNextCursor());
        Assert.assertEquals("30005af19b3800000000000000000000", recordEntities.get(0).getCursor());
        Assert.assertEquals(1525783352873L, recordEntities.get(0).getSystemTime());
        Assert.assertEquals("value1", recordEntities.get(0).getAttributes().get("key1"));
        Assert.assertEquals("value2", recordEntities.get(0).getAttributes().get("key2"));
        BlobRecordData blobRecordData = (BlobRecordData)recordEntities.get(0).getRecordData();
        Assert.assertEquals("AAAA", new String(blobRecordData.getData()));
    }

    @Test
    public void testGetTupleRecords() {
        // get tuple
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("field1", FieldType.STRING));
        schema.addField(new Field("field2", FieldType.BIGINT));

        mockServerClient.when(request(), Times.exactly(1)).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"NextCursor\":\"30005af19b3800000000000000090001\",\"RecordCount\":1,\"StartSeq\":1,\"Records\":[{\"SystemTime\":1525783352873,\"NextCursor\":\"30005af19b3800000000000000010000\",\"Cursor\":\"30005af19b3800000000000000000000\",\"Sequence\":0,\"Data\":[\"AAAA\",\"100\"],\"Attributes\":{\"key1\":\"value1\", \"key2\":\"value2\"}}]}")
        );

        GetRecordsResult getRecordsResult = client.getRecords("test_project", "test_topic", "0", schema, "30005af19b3800000000000000000000", 1);
        TupleRecordData tupleRecordData = (TupleRecordData)getRecordsResult.getRecords().get(0).getRecordData();
        Assert.assertEquals("AAAA", tupleRecordData.getField(0));
        Assert.assertEquals(100L, tupleRecordData.getField("field2"));


        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/shards/0")
                        .withBody(exact("{\"Action\":\"sub\",\"Cursor\":\"30005af19b3800000000000000000000\",\"Limit\":1}"))
        );
    }

    @Test
    public void testGetTupleWithNoSchema() {
        try {
            GetRecordsResult getRecordsResult = client.getRecords("test_project", "test_topic", "0", null, "30005af19b3800000000000000000000", 1);
            fail("Put records with null should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testGetTupleWithNullRecordDataReturn() {
        // get tuple
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("field1", FieldType.STRING));
        schema.addField(new Field("field2", FieldType.BIGINT));

        mockServerClient.when(request(), Times.exactly(1)).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"NextCursor\":\"30005af19b3800000000000000090001\",\"RecordCount\":1,\"StartSeq\":1,\"Records\":[{\"SystemTime\":1525783352873,\"NextCursor\":\"30005af19b3800000000000000010000\",\"Cursor\":\"30005af19b3800000000000000000000\",\"Sequence\":0}]}")
        );

        try {
            GetRecordsResult getRecordsResult = client.getRecords("test_project", "test_topic", "0", schema,"30005af19b3800000000000000000000", 1);
            fail("GetTupleWithNullRecordData should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("BLOB"));
        }
    }

    @Test
    public void testGetTupleWithBlobRecordDataReturn() {
        // get tuple
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("field1", FieldType.STRING));
        schema.addField(new Field("field2", FieldType.BIGINT));

        mockServerClient.when(request(), Times.exactly(1)).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"NextCursor\":\"30005af19b3800000000000000090001\",\"RecordCount\":1,\"StartSeq\":1,\"Records\":[{\"SystemTime\":1525783352873,\"NextCursor\":\"30005af19b3800000000000000010000\",\"Cursor\":\"30005af19b3800000000000000000000\",\"Sequence\":0,\"Data\":\"QUFBQQ==\"}]}")
        );

        try {
            GetRecordsResult getRecordsResult = client.getRecords("test_project", "test_topic", "0", schema, "30005af19b3800000000000000000000", 1);
            fail("GetTupleWithNullRecordData should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("BLOB"));
        }
    }

}
