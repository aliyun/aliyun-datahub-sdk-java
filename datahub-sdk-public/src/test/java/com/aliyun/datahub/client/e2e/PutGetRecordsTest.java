package com.aliyun.datahub.client.e2e;

import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.MalformedRecordException;
import com.aliyun.datahub.client.model.*;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

public class PutGetRecordsTest extends BaseTest {

    @BeforeClass
    public static void setUpBeforeClass() {
        bCreateTopic = true;
        BaseTest.setUpBeforeClass();
    }

    private RecordEntry genTupleData(RecordSchema schema, final String shardId) {
        final TupleRecordData data = new TupleRecordData(schema);
        data.setField("a", "string");
        data.setField("b", 5L);
        data.setField("c", 0.0);
        data.setField("d", 123456789000000L);
        data.setField("e", true);
        data.setField("f", new BigDecimal(10000.000001));

        return new RecordEntry() {{
            addAttribute("partition", "ds=2016");
            setRecordData(data);
            setShardId(shardId);
        }};
    }

    private RecordEntry genTupleDataWithPartitionKey(RecordSchema schema, final String partitionKey) {
        final TupleRecordData data = new TupleRecordData(schema);
        data.setField("a", "string");
        data.setField("b", 5L);
        data.setField("c", 0.0);
        data.setField("d", 123456789000000L);
        data.setField("e", true);
        data.setField("f", new BigDecimal(10000.000001));

        return new RecordEntry() {{
            addAttribute("partition", "ds=2016");
            setRecordData(data);
            setPartitionKey(partitionKey);
        }};
    }

    private RecordEntry genTupleDataWithHashKey(RecordSchema schema, final String hashKey) {
        final TupleRecordData data = new TupleRecordData(schema);
        data.setField("a", "string");
        data.setField("b", 5L);
        data.setField("c", 0.0);
        data.setField("d", 123456789000000L);
        data.setField("e", true);
        data.setField("f", new BigDecimal(10000.000001));

        return new RecordEntry() {{
            addAttribute("partition", "ds=2016");
            setRecordData(data);
            setHashKey(hashKey);
        }};
    }

    private RecordEntry genTupleDataWithLessField(RecordSchema schema, final String shardId) {
        final TupleRecordData data = new TupleRecordData(schema);
        data.setField("a", "string");
        data.setField("b", 5L);
        data.setField("c", 0.0);
        data.setField("d", 123456789000000L);
        data.setField("e", true);

        return new RecordEntry() {{
            addAttribute("partition", "ds=2016");
            setRecordData(data);
            setShardId(shardId);
        }};
    }

    private RecordEntry genBlobData(final String shardId) {
        final BlobRecordData data = new BlobRecordData("test-data".getBytes());
        return new RecordEntry() {{
            addAttribute("partition", "ds=2016");
            setRecordData(data);
            setShardId(shardId);
        }};
    }

    @Test
    public void testPutTupleRecords() {
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            RecordEntry entry = genTupleData(schema, String.valueOf(i % SHARD_COUNT));
            recordEntries.add(entry);
        }

        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(0, putRecordsResult.getFailedRecordCount());

        // get tuple records
        GetCursorResult getCursorResult = client.getCursor(TEST_PROJECT_NAME, tupleTopicName, "0", CursorType.OLDEST);
        GetRecordsResult getRecordsResult = client.getRecords(TEST_PROJECT_NAME, tupleTopicName, "0", schema, getCursorResult.getCursor(), 10);
        Assert.assertEquals(4, getRecordsResult.getRecordCount());
        TupleRecordData data = (TupleRecordData)getRecordsResult.getRecords().get(0).getRecordData();
        Assert.assertEquals("ds=2016", getRecordsResult.getRecords().get(0).getAttributes().get("partition"));
        Assert.assertEquals("string", data.getField(0));
        Assert.assertEquals(5L, data.getField(1));
        Assert.assertEquals(0.0, data.getField(2));
        Assert.assertEquals(123456789000000L, data.getField(3));
        Assert.assertEquals(true, data.getField(4));
        Assert.assertEquals(new BigDecimal(10000.000001), data.getField(5));
    }

    @Test
    public void testPutTupleRecordsByShard() {
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            RecordEntry entry = genTupleData(schema, String.valueOf(i % SHARD_COUNT));
            if (i % 2 == 0) {
                entry.setShardId(null);
            }
            recordEntries.add(entry);
        }
        PutRecordsByShardResult putRecordsByShardResult = client.putRecordsByShard(TEST_PROJECT_NAME, tupleTopicName, "0", recordEntries);
        // get tuple records
        GetCursorResult getCursorResult = client.getCursor(TEST_PROJECT_NAME, tupleTopicName, "0", CursorType.OLDEST);
        GetRecordsResult getRecordsResult = client.getRecords(TEST_PROJECT_NAME, tupleTopicName, "0", schema, getCursorResult.getCursor(), 10);
        Assert.assertEquals(10, getRecordsResult.getRecordCount());
        TupleRecordData data = (TupleRecordData)getRecordsResult.getRecords().get(0).getRecordData();
        Assert.assertEquals("ds=2016", getRecordsResult.getRecords().get(0).getAttributes().get("partition"));
        Assert.assertEquals("string", data.getField(0));
        Assert.assertEquals(5L, data.getField(1));
        Assert.assertEquals(0.0, data.getField(2));
        Assert.assertEquals(123456789000000L, data.getField(3));
        Assert.assertEquals(true, data.getField(4));
        Assert.assertEquals(new BigDecimal(10000.000001), data.getField(5));
    }

    @Test
    public void testPutBlobRecords() {
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            RecordEntry entry = genBlobData(String.valueOf(i % SHARD_COUNT));
            recordEntries.add(entry);
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, blobTopicName, recordEntries);
        Assert.assertEquals(0, putRecordsResult.getFailedRecordCount());

        // get blob records
        GetCursorResult getCursorResult = client.getCursor(TEST_PROJECT_NAME, blobTopicName, "0", CursorType.OLDEST);
        GetRecordsResult getRecordsResult = client.getRecords(TEST_PROJECT_NAME, blobTopicName, "0", getCursorResult.getCursor(), 10);
        Assert.assertEquals(4, getRecordsResult.getRecordCount());
        BlobRecordData data = (BlobRecordData)getRecordsResult.getRecords().get(0).getRecordData();
        Assert.assertEquals("test-data".getBytes(), data.getData());
        Assert.assertEquals("ds=2016", getRecordsResult.getRecords().get(0).getAttributes().get("partition"));
    }

    @Test
    public void testPutBlobRecordsByShard() {
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            RecordEntry entry = genBlobData(String.valueOf(i % SHARD_COUNT));
            if (i % 2 == 0) {
                entry.setShardId(null);
            }
            recordEntries.add(entry);
        }
        client.putRecordsByShard(TEST_PROJECT_NAME, blobTopicName, "0", recordEntries);

        // get blob records
        GetCursorResult getCursorResult = client.getCursor(TEST_PROJECT_NAME, blobTopicName, "0", CursorType.OLDEST);
        GetRecordsResult getRecordsResult = client.getRecords(TEST_PROJECT_NAME, blobTopicName, "0", getCursorResult.getCursor(), 10);
        Assert.assertEquals(10, getRecordsResult.getRecordCount());
        BlobRecordData data = (BlobRecordData)getRecordsResult.getRecords().get(0).getRecordData();
        Assert.assertEquals("test-data".getBytes(), data.getData());
        Assert.assertEquals("ds=2016", getRecordsResult.getRecords().get(0).getAttributes().get("partition"));
    }

    @Test
    public void testPutWithInvalidShard() {
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            RecordEntry entry = genBlobData("4");
            recordEntries.add(entry);
        }

        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, blobTopicName, recordEntries);
        Assert.assertEquals(10, putRecordsResult.getFailedRecordCount());
        for (PutErrorEntry errorEntry : putRecordsResult.getPutErrorEntries()) {
            Assert.assertEquals("NoSuchShard", errorEntry.getErrorcode());
        }
    }

    @Test
    public void testPutWithInvalidShardFormat() {
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            RecordEntry entry = genBlobData("AAA");
            recordEntries.add(entry);
        }

        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, blobTopicName, recordEntries);
        Assert.assertEquals(10, putRecordsResult.getFailedRecordCount());
        for (PutErrorEntry errorEntry : putRecordsResult.getPutErrorEntries()) {
            Assert.assertEquals("InvalidShardId", errorEntry.getErrorcode());
        }
    }

    @Test
    public void testPutSealedShard() {
        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            //
        }
        client.splitShard(TEST_PROJECT_NAME, blobTopicName, "0");

        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            RecordEntry entry = genBlobData("0");
            recordEntries.add(entry);
        }

        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, blobTopicName, recordEntries);
        Assert.assertEquals(10, putRecordsResult.getFailedRecordCount());
        for (PutErrorEntry errorEntry : putRecordsResult.getPutErrorEntries()) {
            Assert.assertEquals("InvalidShardOperation", errorEntry.getErrorcode());
        }

    }

    @Test
    public void testPutWithMoreField() {
        List<RecordEntry> recordEntries = new ArrayList<>();
        schema.addField(new Field("add_field", FieldType.STRING));
        for (int i = 0; i < 10; ++i) {
            RecordEntry entry = genTupleData(schema, "0");
            TupleRecordData data = (TupleRecordData) entry.getRecordData();
            data.setField("add_field", "add_field");
            recordEntries.add(entry);
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(10, putRecordsResult.getFailedRecordCount());
        for (PutErrorEntry errorEntry : putRecordsResult.getPutErrorEntries()) {
            Assert.assertEquals("MalformedRecord", errorEntry.getErrorcode());
        }
    }

    @Test
    public void testPutByShardWithMoreField() {
        List<RecordEntry> recordEntries = new ArrayList<>();
        schema.addField(new Field("add_field", FieldType.STRING));
        for (int i = 0; i < 10; ++i) {
            RecordEntry entry = genTupleData(schema, "0");
            TupleRecordData data = (TupleRecordData) entry.getRecordData();
            data.setField("add_field", "add_field");
            recordEntries.add(entry);
        }

        try {
            client.putRecordsByShard(TEST_PROJECT_NAME, tupleTopicName, "0", recordEntries);
            fail("should throw MalformedRecordException");
        } catch (DatahubClientException e) {
            Assert.assertTrue(e instanceof MalformedRecordException);
        }
    }

    @Test
    public void testWithLessField() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("a", FieldType.STRING));
            addField(new Field("b", FieldType.BIGINT));
            addField(new Field("c", FieldType.DOUBLE));
            addField(new Field("d", FieldType.TIMESTAMP));
            addField(new Field("e", FieldType.BOOLEAN));
        }};

        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            RecordEntry entry = genTupleDataWithLessField(schema, "0");
            recordEntries.add(entry);
        }

        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(10, putRecordsResult.getFailedRecordCount());
        for (PutErrorEntry errorEntry : putRecordsResult.getPutErrorEntries()) {
            Assert.assertEquals("MalformedRecord", errorEntry.getErrorcode());
        }
    }

    @Test
    public void testPutByShardWithLessField() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("a", FieldType.STRING));
            addField(new Field("b", FieldType.BIGINT));
            addField(new Field("c", FieldType.DOUBLE));
            addField(new Field("d", FieldType.TIMESTAMP));
            addField(new Field("e", FieldType.BOOLEAN));
        }};

        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            RecordEntry entry = genTupleDataWithLessField(schema, "0");
            recordEntries.add(entry);
        }

        try {
            client.putRecordsByShard(TEST_PROJECT_NAME, tupleTopicName, "0", recordEntries);
            fail("should throw MalformedRecordException");
        } catch (DatahubClientException e) {
            Assert.assertTrue(e instanceof MalformedRecordException);
        }
    }

    @Test
    public void testPutBLobDataIntoTupleTopic() {
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            RecordEntry entry = genBlobData("0");
            recordEntries.add(entry);
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(10, putRecordsResult.getFailedRecordCount());
        for (PutErrorEntry errorEntry : putRecordsResult.getPutErrorEntries()) {
            Assert.assertEquals("MalformedRecord", errorEntry.getErrorcode());
        }
    }

    @Test
    public void testPutTupleDataIntoBlobTopic() {
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            RecordEntry entry = genTupleData(schema, "0");
            recordEntries.add(entry);
        }

        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, blobTopicName, recordEntries);
        Assert.assertEquals(10, putRecordsResult.getFailedRecordCount());
        for (PutErrorEntry errorEntry : putRecordsResult.getPutErrorEntries()) {
            Assert.assertEquals("MalformedRecord", errorEntry.getErrorcode());
        }
    }

    @Test
    public void testPutNullValue() {
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            RecordEntry entry = genTupleData(schema, "0");
            // set first and second field is null
            TupleRecordData data = (TupleRecordData) entry.getRecordData();
            data.setField(0, null);
            data.setField(1, null);
            recordEntries.add(entry);
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(0, putRecordsResult.getFailedRecordCount());

        // get records
        // get tuple records
        GetCursorResult getCursorResult = client.getCursor(TEST_PROJECT_NAME, tupleTopicName, "0", CursorType.OLDEST);
        GetRecordsResult getRecordsResult = client.getRecords(TEST_PROJECT_NAME, tupleTopicName, "0", schema, getCursorResult.getCursor(), 10);
        Assert.assertEquals(10, getRecordsResult.getRecordCount());
        for (RecordEntry entry : getRecordsResult.getRecords()) {
            TupleRecordData data = (TupleRecordData)entry.getRecordData();
            Assert.assertNull(data.getField(0));
            Assert.assertNull(data.getField(1));
            Assert.assertEquals(0.0, data.getField(2));
            Assert.assertEquals(123456789000000L, data.getField(3));
            Assert.assertEquals(true, data.getField(4));
            Assert.assertEquals(new BigDecimal(10000.000001), data.getField(5));
        }
    }

    @Test
    public void testPutWithPartitionKey() {
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            RecordEntry entry = genTupleDataWithPartitionKey(schema, String.valueOf(i));
            recordEntries.add(entry);
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(0, putRecordsResult.getFailedRecordCount());
    }

    @Test
    public void testPutWithHashKey() {
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            RecordEntry entry = genTupleDataWithHashKey(schema, md5Signature(String.valueOf(i)));
            recordEntries.add(entry);
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(0, putRecordsResult.getFailedRecordCount());
    }

    @Test
    public void testPutRecordsAfterAppendField() {
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            RecordEntry entry = genTupleData(schema, "0");
            recordEntries.add(entry);
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(0, putRecordsResult.getFailedRecordCount());

        client.appendField(TEST_PROJECT_NAME, tupleTopicName, new Field("appendfield", FieldType.STRING));

        RecordSchema recordSchema = client.getTopic(TEST_PROJECT_NAME, tupleTopicName).getRecordSchema();
        recordEntries = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            RecordEntry entry = genTupleData(recordSchema, "0");
            TupleRecordData data = (TupleRecordData) entry.getRecordData();
            data.setField("appendfield", "test");
            recordEntries.add(entry);
        }
        putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(0, putRecordsResult.getFailedRecordCount());

        // get records
        GetCursorResult getCursorResult = client.getCursor(TEST_PROJECT_NAME, tupleTopicName, "0", CursorType.OLDEST);
        GetRecordsResult getRecordsResult = client.getRecords(TEST_PROJECT_NAME, tupleTopicName, "0", recordSchema, getCursorResult.getCursor(), 20);
        Assert.assertEquals(20, getRecordsResult.getRecordCount());
        for (int i = 0; i < 20; ++i) {
            TupleRecordData data = (TupleRecordData) getRecordsResult.getRecords().get(i).getRecordData();
            if (i < 10) {
                Assert.assertNull(data.getField("appendfield"));
            } else {
                Assert.assertEquals("test", data.getField("appendfield"));
            }
        }

    }

    private static String md5Signature(String message) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] array = md.digest(message.getBytes());
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < array.length; ++i) {
                sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1, 3));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
        }
        return null;

    }

}
