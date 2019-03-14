package com.aliyun.datahub.oldsdk;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.exception.LimitExceededException;
import com.aliyun.datahub.model.*;
import com.aliyun.datahub.model.compress.CompressionFormat;
import com.aliyun.datahub.oldsdk.util.DatahubTestUtils;
import com.aliyun.datahub.wrapper.Project;
import com.aliyun.datahub.wrapper.Topic;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class BlobTopicTest {
    private String projectName = null;
    private String topicName = null;
    private DatahubClient client = null;
    private Project project = null;
    private Topic topic = null;
    private int shardCount = 3;
    private String data = String.valueOf(System.currentTimeMillis());
    private CompressionFormat compressionFormat = null;

    BlobTopicTest() {
        this.compressionFormat = CompressionFormat.LZ4;
    }

    BlobTopicTest(CompressionFormat compressionFormat) {
        this.compressionFormat = compressionFormat;
    }

    @BeforeClass
    public void init() {
        DatahubConfiguration conf = DatahubTestUtils.getConf();
        conf.setCompressionFormat(compressionFormat);
        projectName = DatahubTestUtils.getProjectName();
        client = new DatahubClient(conf);
        project = Project.Builder.build(projectName, client);
    }

    @BeforeMethod
    public void setUp() {
        topicName = DatahubTestUtils.getRandomTopicName();
        shardCount = 3;
        int lifeCycle = 3;
        RecordType type = RecordType.BLOB;
        String comment = "";
        topic = project.createTopic(topicName, shardCount, lifeCycle, type, null, comment);
        topic.waitForShardReady();
    }

    @AfterMethod (alwaysRun = true)
    public void tearDown() {
        project.deleteTopic(topicName);
    }

    private BlobRecordEntry genData() {
        BlobRecordEntry entry = new BlobRecordEntry();
        entry.setData(data.getBytes());
        return entry;
    }

    private String genRandomString() {
        String dataStr = RandomStringUtils.random(1024*16, 0, 255, false, false);
        BlobRecordEntry entry = new BlobRecordEntry();
        entry.setData(dataStr.getBytes());
        return dataStr;
    }

    private RecordEntry genData(RecordSchema schema) {
        RecordEntry entry = new RecordEntry(schema);
        entry.setBigint("b", 5L);
        entry.setDouble("c", 0.0);
        entry.setTimeStamp("d", 123456789000000L);
        entry.setBoolean("e", true);
        entry.putAttribute("partition", "ds=2016");
        return entry;
    }

    @Test
    public void testPutBlobRecords() {
        List<BlobRecordEntry> recordEntries = new ArrayList<BlobRecordEntry>();
        int recordNum = 10;
        for (int n = 0; n < recordNum; n++) {
            BlobRecordEntry entry = genData();
            String shardId = String.valueOf(n % shardCount);
            entry.setShardId(shardId);
            recordEntries.add(entry);
        }
        PutBlobRecordsResult result = client.putBlobRecords(projectName, topicName, recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), 0);
        GetCursorResult c1 = client.getCursor(projectName, topicName, "0", GetCursorRequest.CursorType.OLDEST);
        GetBlobRecordsResult r1 = client.getBlobRecords(projectName, topicName, "0", c1.getCursor(), recordNum);
        Assert.assertEquals(r1.getRecords().get(0).getData(), data.getBytes());
    }

    @Test
    public void testPutEmptyBlobRecords() {
        List<BlobRecordEntry> recordEntries = new ArrayList<BlobRecordEntry>();
        int recordNum = 10;
        for (int n = 0; n < recordNum; n++) {
            BlobRecordEntry entry = new BlobRecordEntry();
            entry.setData("".getBytes());
            String shardId = String.valueOf(n % shardCount);
            entry.setShardId(shardId);
            recordEntries.add(entry);
        }
        PutBlobRecordsResult result = client.putBlobRecords(projectName, topicName, recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), 0);
        GetCursorResult c1 = client.getCursor(projectName, topicName, "0", GetCursorRequest.CursorType.OLDEST);
        GetBlobRecordsResult r1 = client.getBlobRecords(projectName, topicName, "0", c1.getCursor(), recordNum);
        Assert.assertEquals(r1.getRecords().get(0).getData(), "".getBytes());
    }

    @Test(enabled = false, expectedExceptions = LimitExceededException.class)
    public void testBodyExceededLimitValue() {
        List<BlobRecordEntry> recordEntries = new ArrayList<BlobRecordEntry>();
        for (int i = 0; i < 5; i++) {
            BlobRecordEntry entry = new BlobRecordEntry();
            entry.setShardId("0");
            entry.setData(DatahubTestUtils.getRandomString(DatahubTestUtils.MAX_STRING_LENGTH - 1).getBytes());
            recordEntries.add(entry);
        }
        topic.putBlobRecords(recordEntries);
    }

    @Test
    public void testPutTupleRecordsInBlobTopic() {

        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("b", FieldType.BIGINT));
        schema.addField(new Field("c", FieldType.DOUBLE));
        schema.addField(new Field("d", FieldType.TIMESTAMP));
        schema.addField(new Field("e", FieldType.BOOLEAN));

        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
        int recordNum = 10;

        for (int n = 0; n < recordNum; n++) {
            RecordEntry entry = genData(schema);
            String shardId = String.valueOf(n % shardCount);
            entry.setShardId(shardId);
            entry.putAttribute("partition", "ds=2016");

            recordEntries.add(entry);
        }
        PutRecordsResult result = client.putRecords(projectName, topicName, recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), recordNum);

        for (ErrorEntry e :result.getFailedRecordError()) {
            Assert.assertEquals(e.getErrorcode(), "MalformedRecord");
        }
    }

    @Test
    public void testRandomPutBlobRecords() {
        String cursor = null;
        for(int i = 0; i < 1000; i++) {
            List<BlobRecordEntry> recordEntries = new ArrayList<BlobRecordEntry>();
            int recordNum = 1;
            String tmpString = genRandomString();
            for (int n = 0; n < recordNum; n++) {
                BlobRecordEntry entry = new BlobRecordEntry();
                entry.setData(tmpString.getBytes());
                entry.setShardId("0");
                recordEntries.add(entry);
            }
            PutBlobRecordsResult result = client.putBlobRecords(projectName, topicName, recordEntries);
            Assert.assertEquals(result.getFailedRecordCount(), 0);
            if (cursor == null) {
                GetCursorResult c1 = client.getCursor(projectName, topicName, "0", GetCursorRequest.CursorType.OLDEST);
                cursor = c1.getCursor();
            }
            GetBlobRecordsResult r1 = client.getBlobRecords(projectName, topicName, "0", cursor, recordNum);
            for (BlobRecordEntry recordEntry : r1.getRecords()) {
                Assert.assertEquals(recordEntry.getData(), tmpString.getBytes(), tmpString);
            }
            cursor = r1.getNextCursor();
        }
    }
}
