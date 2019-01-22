package com.aliyun.datahub;

import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.util.KeyRangeUtils;
import com.aliyun.datahub.exception.LimitExceededException;
import com.aliyun.datahub.model.*;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.model.compress.CompressionFormat;
import com.aliyun.datahub.util.DatahubTestUtils;
import com.aliyun.datahub.wrapper.Project;
import com.aliyun.datahub.wrapper.Topic;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
@Test
public class PutRecordsTest {
    private String projectName = null;
    private String topicName = null;
    private Project project = null;
    private Topic topic = null;
    private int shardCount = 3;
    private int lifeCycle = 7;
    private CompressionFormat compressionFormat = null;

    PutRecordsTest() {
        this.compressionFormat = CompressionFormat.LZ4;
    }

    PutRecordsTest(CompressionFormat compressionFormat) {
        this.compressionFormat = compressionFormat;
    }

    @BeforeClass
    public void init() {
        DatahubConfiguration conf = DatahubTestUtils.getConf();
        conf.setCompressionFormat(compressionFormat);
        projectName = DatahubTestUtils.getProjectName();
        DatahubClient client = new DatahubClient(conf);
        project = Project.Builder.build(projectName, client);
    }

    @BeforeMethod
    public void setUp() {
        topicName = DatahubTestUtils.getRandomTopicName();
        shardCount = 3;
        int lifeCycle = 3;
        RecordType type = RecordType.TUPLE;
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("a", FieldType.STRING));
        schema.addField(new Field("b", FieldType.BIGINT));
        schema.addField(new Field("c", FieldType.DOUBLE));
        schema.addField(new Field("d", FieldType.TIMESTAMP));
        schema.addField(new Field("e", FieldType.BOOLEAN));
        schema.addField(new Field("f", FieldType.DECIMAL));
        String comment = "";
        topic = project.createTopic(topicName, shardCount, lifeCycle, type, schema, comment);
        topic.waitForShardReady();
    }

    @AfterMethod (alwaysRun = true)
    public void tearDown() {
        project.deleteTopic(topicName);
    }

    private RecordEntry genData(RecordSchema schema) {
        RecordEntry entry = new RecordEntry(schema);
        entry.setString("a", "string");
        entry.setBigint("b", 5L);
        entry.setDouble("c", 0.0);
        entry.setTimeStamp("d", 123456789000000L);
        entry.setBoolean("e", true);
        entry.setDecimal("f", new BigDecimal(10000.000001));
        entry.putAttribute("partition", "ds=2016");
        return entry;
    }

    private RecordEntry genInvalidData(RecordSchema schema) {
        RecordEntry entry = new RecordEntry(schema);
        entry.setString("a", "string");
        entry.setBigint("b", 5L);
        entry.setDouble("c", 0.0);
        entry.setTimeStamp("d", 123456789000000L);
        entry.putAttribute("partition", "ds=2016");
        return entry;
    }

    private BlobRecordEntry genData() {
        BlobRecordEntry entry = new BlobRecordEntry();
        entry.setData("string".getBytes());
        return entry;
    }

    private RecordEntry genDataWithPartitionKey(RecordSchema schema, String partitionKey) {
        RecordEntry entry = genData(schema);
        entry.setPartitionKey(partitionKey);
        return entry;
    }

    private RecordEntry genDataWithHashValue(RecordSchema schema, String hashValue) {
        RecordEntry entry = genData(schema);
        entry.setHashKey(hashValue);
        return entry;
    }

    private List<RecordEntry> genDataListWithPartitionKey(RecordSchema schema, long recordNum)
    {
        List<RecordEntry> records = new ArrayList<RecordEntry>();
        for (long n = 0; n < recordNum; n++) {
            RecordEntry entry = genDataWithPartitionKey(schema, String.valueOf(n));
            records.add(entry);
        }
        return records;
    }

    private List<RecordEntry> genDataListWithHashValue(RecordSchema schema, long recordNum)
    {
        List<RecordEntry> records = new ArrayList<RecordEntry>();
        for (long n = 0; n < recordNum; n++) {
            RecordEntry entry = genDataWithHashValue(schema, KeyRangeUtils.md5Signature(String.valueOf(String.valueOf(n))));
            records.add(entry);
        }
        return records;
    }

    private void calRecordsInShard(List<ShardEntry> shards, long recordNum, Map<String, Long> recordInShard)
    {
        for (long n = 0; n < recordNum; n++) {
            calRecordCountPerShard(shards, KeyRangeUtils.md5Signature(String.valueOf(n)), recordInShard);
        }
    }

    private void calRecordCountPerShard(List<ShardEntry> shards, String hashValue, Map<String, Long> recordInShard)
    {
        BigInteger hashKey = KeyRangeUtils.hashKeyToBigInt(hashValue);
        String shardId = null;
        BigInteger maxBeginKey = null;
        for (ShardEntry e : shards) {
            if (e.getClosedTime() != 0) {
                continue;
            }
            BigInteger begin = KeyRangeUtils.hashKeyToBigInt(e.getBeginHashKey());
            if (hashKey.compareTo(begin) >= 0
                    && (maxBeginKey == null || begin.compareTo(maxBeginKey) > 0)) {
                shardId = e.getShardId();
                maxBeginKey = begin;
            }
        }
        if (shardId == null) {
            // never reach here by design
            System.out.println("Can not find shard to put.");
            Assert.assertTrue(false);
        }
        if (recordInShard.containsKey(shardId)) {
            Long cur = recordInShard.get(shardId) + 1;
            recordInShard.remove(shardId);
            recordInShard.put(shardId, cur);
        } else {
            recordInShard.put(shardId, 1L);
        }
    }

    private void checkRecordInCorrectShard(Topic topic, Map<String, Long> recordInShard, int limit) {
        List<ShardEntry> shards = topic.listShard();
        for (ShardEntry e : shards) {
            String cursor = topic.getCursor(e.getShardId(), GetCursorRequest.CursorType.OLDEST);
            GetRecordsResult getRecordsResult = topic.getRecords(e.getShardId(), cursor, limit);
            Assert.assertEquals(getRecordsResult.getRecordCount(), recordInShard.get(e.getShardId()).intValue());
        }
    }

    @Test
    public void testPutRecordsRecreateTopic() {
        RecordSchema schema = topic.getRecordSchema();
        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
        int recordNum = 10;

        for (long n = 0; n < recordNum; n++) {
            RecordEntry entry = genData(schema);
            String shardId = String.valueOf(n % shardCount);
            entry.setShardId(shardId);

            recordEntries.add(entry);
        }
        PutRecordsResult result = topic.putRecords(recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), 0);

        project.deleteTopic(topicName);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {

        }
        topic = project.createTopic(topicName, shardCount, lifeCycle, RecordType.TUPLE, schema, "");
        topic.waitForShardReady();
        result = topic.putRecords(recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), recordNum);
        Assert.assertEquals(result.getFailedRecordError().get(0).getErrorcode(), "InternalServerError");
        Assert.assertEquals(result.getFailedRecordError().get(0).getMessage(), "Service topic meta cache refreshing, please try again later.");
    }

    @Test
    public void testPutRecords() {
        RecordSchema schema = topic.getRecordSchema();
        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
        long recordNum = 10;

        for (long n = 0; n < recordNum; n++) {
            RecordEntry entry = genData(schema);
            String shardId = String.valueOf(n % shardCount);
            entry.setShardId(shardId);
            entry.putAttribute("partition", "ds=2016");

            recordEntries.add(entry);
        }
        PutRecordsResult result = topic.putRecords(recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), 0);
    }

    @Test
    public void testPutRecordsMoreField() {
        RecordSchema schema = topic.getRecordSchema();
        schema.addField(new Field("add_col", FieldType.STRING));
        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
        long recordNum = 10;

        for (long n = 0; n < recordNum; n++) {
            RecordEntry entry = genData(schema);
            String shardId = String.valueOf(n % shardCount);
            entry.setShardId(shardId);
            entry.putAttribute("partition", "ds=2016");

            recordEntries.add(entry);
        }
        PutRecordsResult result = topic.putRecords(recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), 10);
        for (ErrorEntry e : result.getFailedRecordError()) {
            Assert.assertEquals(e.getErrorcode(), "MalformedRecord");
        }
    }

    @Test
    public void testPutRecordsMissingField() {
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("a", FieldType.STRING));
        schema.addField(new Field("b", FieldType.BIGINT));
        schema.addField(new Field("c", FieldType.DOUBLE));
        schema.addField(new Field("d", FieldType.TIMESTAMP));
        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
        long recordNum = 10;

        for (long n = 0; n < recordNum; n++) {
            RecordEntry entry = genInvalidData(schema);
            String shardId = String.valueOf(n % shardCount);
            entry.setShardId(shardId);
            entry.putAttribute("partition", "ds=2016");

            recordEntries.add(entry);
        }
        PutRecordsResult result = topic.putRecords(recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), 10);
        for (ErrorEntry e : result.getFailedRecordError()) {
            Assert.assertEquals(e.getErrorcode(), "MalformedRecord");
        }
    }

    @Test
    public void testNoneAsciiValue() {
        RecordSchema schema = topic.getRecordSchema();
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        RecordEntry entry = new RecordEntry(schema);
        entry.setString(0, "\u0002");
        entry.setShardId("0");

        List<RecordEntry> recordEntries = Arrays.asList(entry);

        PutRecordsResult result = topic.putRecords(recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), 0);

        String cursor = topic.getCursor("0", GetCursorRequest.CursorType.LATEST);

        GetRecordsResult getRecordsResult = topic.getRecords("0", cursor, 1);

        Assert.assertEquals(getRecordsResult.getRecordCount(), 1);
        Assert.assertEquals(getRecordsResult.getRecords().get(0).getString(0), "\u0002");
    }

    @Test
    public void testPutBlobRecordsInTupleTopic() {
        List<BlobRecordEntry> recordEntries = new ArrayList<BlobRecordEntry>();
        long recordNum = 10;

        for (long n = 0; n < recordNum; n++) {
            BlobRecordEntry entry = genData();
            String shardId = String.valueOf(n % shardCount);
            entry.setShardId(shardId);
            entry.putAttribute("partition", "ds=2016");

            recordEntries.add(entry);
        }
        PutBlobRecordsResult result = topic.putBlobRecords(recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), recordNum);
        for (ErrorEntry e :result.getFailedRecordError()) {
            Assert.assertEquals(e.getErrorcode(), "MalformedRecord");
        }
    }

    @Test
    public void testPutRecordWithRetry() {
        RecordSchema schema = topic.getRecordSchema();

        RecordEntry entry = new RecordEntry(schema);
        entry.setShardId("0");
        entry.setString("a", DatahubTestUtils.getRandomString());
        entry.setBigint("b", DatahubTestUtils.getRandomNumber());
        entry.setDouble("c", DatahubTestUtils.getRandomDecimal());
        entry.setTimeStamp("d", DatahubTestUtils.getRandomTimestamp());
        entry.setBoolean("e", DatahubTestUtils.getRandomBoolean());

        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
        recordEntries.add(entry);

        RecordEntry invalidEntry = new RecordEntry(schema);
        invalidEntry.setShardId("0");
        invalidEntry.setBigint("e", 5L);
        recordEntries.add(invalidEntry);

        PutRecordsResult result = topic.putRecords(recordEntries, 3);
        Assert.assertEquals(1, result.getFailedRecordCount());

        String cursor = topic.getCursor("0", GetCursorRequest.CursorType.OLDEST);

        GetRecordsResult recordsResult = topic.getRecords("0", cursor, 10);
        Assert.assertEquals(1, recordsResult.getRecordCount());
    }

    @Test
    public void testInvalidShard() {
        RecordSchema schema = topic.getRecordSchema();

        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();

        long recordNum = 10;

        for (long n = 0; n < recordNum; n++) {
            RecordEntry entry = genData(schema);
            String shardId = String.valueOf(n % (shardCount + 1));
            entry.setShardId(shardId);
            entry.putAttribute("partition", "ds=2016");

            recordEntries.add(entry);
        }
        PutRecordsResult result = topic.putRecords(recordEntries);

        List<Integer> failedIndex = new ArrayList<Integer>();
        List<RecordEntry> failedRecord = new ArrayList<RecordEntry>();
        failedIndex.add(3);
        failedRecord.add(recordEntries.get(3));
        failedIndex.add(7);
        failedRecord.add(recordEntries.get(7));
        Assert.assertEquals(failedIndex.toArray(), result.getFailedRecordIndex().toArray());
        Assert.assertEquals(failedRecord.toArray(), result.getFailedRecords().toArray());
    }

    @Test
    public void testInvalidShard2() {
        RecordSchema schema = topic.getRecordSchema();

        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();

        int recordNum = 10;
        List<Integer> failedIndex = new ArrayList<Integer>();
        List<RecordEntry> failedRecord = new ArrayList<RecordEntry>();

        for (int n = 0; n < recordNum; n++) {
            RecordEntry entry = genData(schema);
            String shardId = "-1";
            entry.setShardId(shardId);
            recordEntries.add(entry);

            failedIndex.add(n);
            failedRecord.add(entry);
        }
        PutRecordsResult result = topic.putRecords(recordEntries);

        Assert.assertEquals(failedIndex.toArray(), result.getFailedRecordIndex().toArray());
        Assert.assertEquals(failedRecord.toArray(), result.getFailedRecords().toArray());
    }

    @Test(enabled = false, expectedExceptions = LimitExceededException.class)
    public void testBodyExceededLimitValue() {
        RecordSchema schema = topic.getRecordSchema();

        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
        for (int i = 0; i < 5; i++) {
            RecordEntry entry = new RecordEntry(schema);
            entry.setShardId("0");
            entry.setString("a", DatahubTestUtils.getRandomString(DatahubTestUtils.MAX_STRING_LENGTH - 1));
            entry.setBigint("b", Long.MAX_VALUE);
            entry.setDouble("c", Double.MAX_VALUE);
            entry.setTimeStamp("d", DatahubTestUtils.MAX_TIMESTAMP_VALUE);
            entry.setBoolean("e", true);
            recordEntries.add(entry);
        }
        topic.putRecords(recordEntries);
    }

    @Test
    public void testBoundaryValue() {
        RecordSchema schema = topic.getRecordSchema();

        RecordEntry entry = new RecordEntry(schema);
        entry.setShardId("0");
        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
        recordEntries.add(entry);

        recordEntries.get(0).setString("a", DatahubTestUtils.getRandomString(DatahubTestUtils.MAX_STRING_LENGTH));
        recordEntries.get(0).setBigint("b", Long.MAX_VALUE);
        recordEntries.get(0).setDouble("c", Double.MAX_VALUE);
        recordEntries.get(0).setTimeStamp("d", DatahubTestUtils.MAX_TIMESTAMP_VALUE);
        recordEntries.get(0).setBoolean("e", true);

        PutRecordsResult result = topic.putRecords(recordEntries);

        Assert.assertEquals(result.getFailedRecordCount(), 0);

        recordEntries.get(0).setString("a", "");
        recordEntries.get(0).setBigint("b", Long.MIN_VALUE + 1);
        recordEntries.get(0).setDouble("c", Double.MIN_VALUE);
        recordEntries.get(0).setTimeStamp("d", DatahubTestUtils.MIN_TIMESTAMP_VALUE);
        recordEntries.get(0).setBoolean("e", false);

        result = topic.putRecords(recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), 0);

        recordEntries.get(0).setString("a", DatahubTestUtils.getRandomString(DatahubTestUtils.MAX_STRING_LENGTH + 1));
        recordEntries.get(0).setBigint("b", DatahubTestUtils.getRandomNumber());
        recordEntries.get(0).setDouble("c", DatahubTestUtils.getRandomDecimal());
        recordEntries.get(0).setTimeStamp("d", DatahubTestUtils.getRandomTimestamp());
        recordEntries.get(0).setBoolean("e", DatahubTestUtils.getRandomBoolean());

        result = topic.putRecords(recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), 1);

        recordEntries.get(0).setString("a", DatahubTestUtils.getRandomString());
        recordEntries.get(0).setBigint("b", DatahubTestUtils.getRandomNumber());
        recordEntries.get(0).setDouble("c", DatahubTestUtils.getRandomDecimal());
        recordEntries.get(0).setTimeStamp("d", DatahubTestUtils.MAX_TIMESTAMP_VALUE + 1);
        recordEntries.get(0).setBoolean("e", DatahubTestUtils.getRandomBoolean());

        result = topic.putRecords(recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), 1);

        recordEntries.get(0).setString("a", DatahubTestUtils.getRandomString());
        recordEntries.get(0).setBigint("b", DatahubTestUtils.getRandomNumber());
        recordEntries.get(0).setDouble("c", DatahubTestUtils.getRandomDecimal());
        recordEntries.get(0).setTimeStamp("d", DatahubTestUtils.MIN_TIMESTAMP_VALUE - 1);
        recordEntries.get(0).setBoolean("e", DatahubTestUtils.getRandomBoolean());
        result = topic.putRecords(recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), 1);
    }

    @Test
    public void testNullValue() {
        RecordSchema schema = topic.getRecordSchema();
        RecordEntry entry = new RecordEntry(schema);
        entry.setShardId("0");
        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
        recordEntries.add(entry);

        recordEntries.get(0).setString("a", DatahubTestUtils.getRandomString());
        recordEntries.get(0).setBigint("b", DatahubTestUtils.getRandomNumber());
        recordEntries.get(0).setDouble("c", DatahubTestUtils.getRandomDecimal());
        recordEntries.get(0).setTimeStamp("d", DatahubTestUtils.getRandomTimestamp());
        recordEntries.get(0).setBoolean("e", DatahubTestUtils.getRandomBoolean());

        PutRecordsResult result = topic.putRecords(recordEntries);
        Assert.assertEquals(0, result.getFailedRecordCount());

        recordEntries.get(0).setString("a", DatahubTestUtils.getRandomString());
        recordEntries.get(0).setBigint("b", null);
        recordEntries.get(0).setDouble("c", DatahubTestUtils.getRandomDecimal());
        recordEntries.get(0).setTimeStamp("d", DatahubTestUtils.getRandomTimestamp());
        recordEntries.get(0).setBoolean("e", DatahubTestUtils.getRandomBoolean());

        result = topic.putRecords(recordEntries);
        Assert.assertEquals(0, result.getFailedRecordCount());

        recordEntries.get(0).setString("a", DatahubTestUtils.getRandomString());
        recordEntries.get(0).setBigint("b", DatahubTestUtils.getRandomNumber());
        recordEntries.get(0).setDouble("c", null);
        recordEntries.get(0).setTimeStamp("d", DatahubTestUtils.getRandomTimestamp());
        recordEntries.get(0).setBoolean("e", DatahubTestUtils.getRandomBoolean());

        result = topic.putRecords(recordEntries);
        Assert.assertEquals(0, result.getFailedRecordCount());

        recordEntries.get(0).setString("a", DatahubTestUtils.getRandomString());
        recordEntries.get(0).setBigint("b", DatahubTestUtils.getRandomNumber());
        recordEntries.get(0).setDouble("c", DatahubTestUtils.getRandomDecimal());
        recordEntries.get(0).setTimeStamp("d", null);
        recordEntries.get(0).setBoolean("e", DatahubTestUtils.getRandomBoolean());

        result = topic.putRecords(recordEntries);
        Assert.assertEquals(0, result.getFailedRecordCount());

        recordEntries.get(0).setString("a", DatahubTestUtils.getRandomString());
        recordEntries.get(0).setBigint("b", DatahubTestUtils.getRandomNumber());
        recordEntries.get(0).setDouble("c", DatahubTestUtils.getRandomDecimal());
        recordEntries.get(0).setTimeStamp("d", DatahubTestUtils.getRandomTimestamp());
        recordEntries.get(0).setBoolean("e", null);

        result = topic.putRecords(recordEntries);
        Assert.assertEquals(0, result.getFailedRecordCount());
    }

    @Test
    public void testSealedShard() {
        RecordSchema schema = topic.getRecordSchema();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {

        }

        List<ShardDesc> res = topic.splitShard("0");

        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();

        long recordNum = 5;

        for (long n = 0; n < recordNum; n++) {
            RecordEntry entry = genData(schema);
            String shardId = String.valueOf(n % shardCount);
            entry.setShardId(shardId);
            entry.putAttribute("partition", "ds=2016");

            recordEntries.add(entry);
        }
        PutRecordsResult result = topic.putRecords(recordEntries);

        List<Integer> failedIndex = new ArrayList<Integer>();
        failedIndex.add(0);
        failedIndex.add(3);
        Assert.assertEquals(failedIndex.toArray(), result.getFailedRecordIndex().toArray());
        List<ErrorEntry> errorList = result.getFailedRecordError();
        for (ErrorEntry i : errorList) {
            Assert.assertEquals(i.getErrorcode(), "InvalidShardOperation");
        }
    }

    @Test
    public void testPutRecordsWithPartitionKey() {
        RecordSchema schema = topic.getRecordSchema();
        long recordNum = 10;
        List<ShardEntry> shards = topic.listShard();
        Map<String, Long> recordInShard = new HashMap<String, Long>();
        List<RecordEntry> recordEntries = genDataListWithPartitionKey(schema, recordNum);

        calRecordsInShard(shards, recordNum, recordInShard);
        PutRecordsResult result = topic.putRecords(recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), 0);

        checkRecordInCorrectShard(topic, recordInShard, (int)recordNum);
    }

    @Test
    public void testPutRecordsWithHashValue() {
        RecordSchema schema = topic.getRecordSchema();
        long recordNum = 10;
        List<ShardEntry> shards = topic.listShard();
        Map<String, Long> recordInShard = new HashMap<String, Long>();
        List<RecordEntry> recordEntries = genDataListWithHashValue(schema, recordNum);

        calRecordsInShard(shards, recordNum, recordInShard);
        PutRecordsResult result = topic.putRecords(recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), 0);

        checkRecordInCorrectShard(topic, recordInShard, (int) recordNum);
    }

    @Test
    public void testPutRecordsWithBoundaryHashValue() {
        RecordSchema schema = topic.getRecordSchema();
        Map<String, Long> recordInShard = new HashMap<String, Long>();
        List<RecordEntry> recordList = new ArrayList<RecordEntry>();
        List<ShardEntry> shards = topic.listShard();
        for (int i = 0; i < shardCount; ++i) {
            RecordEntry recordEntry1 = genDataWithHashValue(schema, shards.get(i).getBeginHashKey());
            RecordEntry recordEntry2 = genDataWithHashValue(schema, shards.get(i).getEndHashKey());
            recordList.add(recordEntry1);
            recordList.add(recordEntry2);
        }
        PutRecordsResult result = topic.putRecords(recordList);
        Assert.assertEquals(result.getFailedRecordCount(), 0);
        for (int i =0; i < shardCount; ++i) {
            calRecordCountPerShard(shards, shards.get(i).getBeginHashKey(), recordInShard);
            calRecordCountPerShard(shards, shards.get(i).getEndHashKey(), recordInShard);
        }

        checkRecordInCorrectShard(topic, recordInShard, shardCount * 2);
    }

    @Test
    public void testSealedShardWithPartitionKey() {
        RecordSchema schema = topic.getRecordSchema();
        long recordNum = 10;
        List<ShardEntry> shards = topic.listShard();
        Map<String, Long> recordInShard = new HashMap<String, Long>();
        List<RecordEntry> recordEntries = genDataListWithPartitionKey(schema, recordNum);

        calRecordsInShard(shards, recordNum, recordInShard);
        PutRecordsResult result = topic.putRecords(recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), 0);
        checkRecordInCorrectShard(topic, recordInShard, (int) recordNum);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {

        }
        List<ShardDesc> res = topic.splitShard("0");
        // redirect shard, wait for shard active, bug do not use list shard to do this, because listshard will update cache in frontend
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {

        }
        result = topic.putRecords(recordEntries);
        shards = topic.listShard();
        calRecordsInShard(shards, recordNum, recordInShard);
        Assert.assertEquals(result.getFailedRecordCount(), 0);
        checkRecordInCorrectShard(topic, recordInShard, (int) recordNum * 2);
    }

    @Test
    public void testWriteBatchRecordsWithMultiShard() {
        RecordSchema schema = topic.getRecordSchema();
        List<ShardEntry> shards = topic.listShard();

        List<RecordEntry> entries = new ArrayList<RecordEntry>();
        for (int i = 0; i < 100; ++i) {
            RecordEntry entry = new RecordEntry(schema);
            entry.setShardId(shards.get(i % shardCount).getShardId());
            entries.add(entry);
        }

        Assert.assertEquals(0, topic.putRecords(entries).getFailedRecordCount());
    }

    @Test
    public void testSealedShardWithHashValue() {
        RecordSchema schema = topic.getRecordSchema();
        long recordNum = 10;
        List<ShardEntry> shards = topic.listShard();
        Map<String, Long> recordInShard = new HashMap<String, Long>();
        List<RecordEntry> recordEntries = genDataListWithHashValue(schema, recordNum);

        calRecordsInShard(shards, recordNum, recordInShard);
        PutRecordsResult result = topic.putRecords(recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), 0);
        checkRecordInCorrectShard(topic, recordInShard, (int) recordNum);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {

        }
        List<ShardDesc> res = topic.splitShard("0");
        // redirect shard, wait for shard active, bug do not use list shard to do this, because listshard will update cache in frontend
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {

        }
        result = topic.putRecords(recordEntries);
        shards = topic.listShard();
        calRecordsInShard(shards, recordNum, recordInShard);
        Assert.assertEquals(result.getFailedRecordCount(), 0);
        checkRecordInCorrectShard(topic, recordInShard, (int) recordNum * 2);
    }

    @Test (groups = {"notnull"})
    public void testNotnullConstraint() {
        String topicName = DatahubTestUtils.getRandomTopicName();
        shardCount = 3;
        int lifeCycle = 3;
        RecordType type = RecordType.TUPLE;
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("a", FieldType.STRING, true));
        String comment = "topic with not null field";
        topic = project.createTopic(topicName, shardCount, lifeCycle, type, schema, comment);
        topic.waitForShardReady();

        schema = topic.getRecordSchema();
        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
        long recordNum = 10;

        List<Integer> expectFailedIndex = new ArrayList<Integer>();
        for (int n = 0; n < recordNum; n++) {
            RecordEntry entry = new RecordEntry(schema);
            if (n % 2 == 0) {
                entry.setString(0, "test");
            } else {
                expectFailedIndex.add(n);
            }

            String shardId = String.valueOf(n % shardCount);
            entry.setShardId(shardId);
            entry.putAttribute("partition", "ds=2016");

            recordEntries.add(entry);
        }
        PutRecordsResult result = topic.putRecords(recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), 5);
        Assert.assertEquals(result.getFailedRecordIndex(), expectFailedIndex);
        project.deleteTopic(topicName);
    }

    @Test
    public void testRandomShardId() {
        String topicName = DatahubTestUtils.getRandomTopicName();
        shardCount = 3;
        int lifeCycle = 3;
        RecordType type = RecordType.TUPLE;
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("a", FieldType.STRING, true));
        String comment = "topic with not null field";
        topic = project.createTopic(topicName, shardCount, lifeCycle, type, schema, comment);
        topic.waitForShardReady();

        schema = topic.getRecordSchema();
        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
        long recordNum = 10;

        for (int n = 0; n < recordNum; n++) {
            RecordEntry entry = new RecordEntry(schema);
            entry.setString(0, "test");
            recordEntries.add(entry);
        }
        PutRecordsResult result = topic.putRecords(recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), 0);

        List<ShardEntry> shards = topic.listShard();
        int flag = 0;
        for (ShardEntry e : shards) {
            String cursor = topic.getCursor(e.getShardId(), GetCursorRequest.CursorType.OLDEST);
            GetRecordsResult getRecordsResult = topic.getRecords(e.getShardId(), cursor, (int)recordNum);

            if (getRecordsResult.getRecordCount() != 0){
                Assert.assertEquals(getRecordsResult.getRecordCount(), recordNum);
                flag++;
            }
        }

        // one shard has all records
        Assert.assertEquals(flag, 1);
        project.deleteTopic(topicName);
    }
}
