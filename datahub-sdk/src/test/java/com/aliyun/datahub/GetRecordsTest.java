package com.aliyun.datahub;

import com.aliyun.datahub.exception.*;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.model.*;
import com.aliyun.datahub.model.compress.CompressionFormat;
import com.aliyun.datahub.util.DatahubTestUtils;
import com.aliyun.datahub.wrapper.Project;
import com.aliyun.datahub.wrapper.Topic;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
@Test
public class GetRecordsTest {
    private String projectName = null;
    private String topicName = null;
    private DatahubClient client = null;
    private Project project = null;
    private CompressionFormat compressionFormat;

    public GetRecordsTest() {
        this.compressionFormat = CompressionFormat.LZ4;
    }

    public GetRecordsTest(CompressionFormat compressionFormat) {
        this.compressionFormat = compressionFormat;
    }

    @BeforeMethod
    public void SetUp() {
        try {
            DatahubConfiguration conf = DatahubTestUtils.getConf();
            conf.setCompressionFormat(compressionFormat);
            Random random = new Random(System.currentTimeMillis());
            projectName = DatahubTestUtils.getProjectName();
            client = new DatahubClient(conf);
            project = Project.Builder.build(projectName, client);
            this.topicName = DatahubTestUtils.getRandomTopicName();
            int shardCount = 3;
            int lifeCycle = 1;
            RecordType type = RecordType.TUPLE;
            RecordSchema schema = new RecordSchema();
            schema.addField(new Field("test", FieldType.STRING));
            schema.addField(new Field("test2", FieldType.STRING));
            String comment = "";
            client.createTopic(projectName, topicName, shardCount, lifeCycle, type, schema, comment);
            client.waitForShardReady(projectName, topicName);
            System.out.println("topic: " + topicName);
        } catch (Exception e) {
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

    private void putRecords(String shardId, int recordNum) {
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.STRING));
        schema.addField(new Field("test2", FieldType.STRING));
        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();

        for (long n = 0; n < recordNum; n++) {
            //RecordData
            RecordEntry entry = new RecordEntry(schema);

            for (int i = 0; i < entry.getFieldCount(); i++) {
                entry.setString(i, new String("test")+String.valueOf(n));
            }

            entry.setShardId(shardId);

            recordEntries.add(entry);
        }
        PutRecordsResult result = client.putRecords(projectName, topicName, recordEntries);
        Assert.assertEquals(result.getFailedRecordCount(), 0);
    }

    /**
     * use shard id 0
     */
    @Test
    public void testGetRecordsByC2() {

        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.STRING));
        putRecords("0", 3);
        GetCursorResult c1 = client.getCursor(projectName, topicName, "0", GetCursorRequest.CursorType.OLDEST);
        GetRecordsResult r1 = client.getRecords(projectName, topicName, "0", c1.getCursor(), 10, schema);

        Assert.assertEquals(r1.getRecordCount(), 3);

        GetCursorResult c2 = client.getCursor(projectName, topicName, "0", GetCursorRequest.CursorType.LATEST);
        GetRecordsResult r2 = client.getRecords(projectName, topicName, "0", c2.getCursor(), 10, schema);

        Assert.assertEquals(r2.getRecordCount(), 1);

        Assert.assertEquals(r1.getRecords().get(2).getString(0), r2.getRecords().get(0).getString(0));

        GetRecordsResult r3 = client.getRecords(projectName, topicName, "0", c1.getCursor(), 2, schema);

        Assert.assertEquals(r1.getRecords().get(1).getString(0), r3.getRecords().get(1).getString(0));
    }

    /**
     * use shard id 0
     */
    @Test
    public void testGetRecordsByC3() {

        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.STRING));
        putRecords("0", 3);
        GetCursorResult c1 = client.getCursor(projectName, topicName, "0", GetCursorRequest.CursorType.LATEST);
        GetRecordsResult r1 = client.getRecords(projectName, topicName, "0", c1.getCursor(), 10, schema);

        String nc = r1.getNextCursor();

        GetRecordsResult r2 = client.getRecords(projectName, topicName, "0", nc, 10, schema);

        Assert.assertEquals(r2.getRecordCount(), 0);

        Assert.assertEquals(r2.getNextCursor(), nc);
    }

    /**
     * use shard id 0
     */
    @Test (expectedExceptions = InvalidCursorException.class)
    public void testGetRecordsByC4() {
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.STRING));
        putRecords("0", 3);
        String invalidCursor = "fffffffffffffffe";
        GetRecordsResult r1 = client.getRecords(projectName, topicName, "0", invalidCursor, 10, schema);
    }

    /**
     * use shard id 0
     */
    @Test (expectedExceptions = InvalidCursorException.class)
    public void testInvalidLengthCursor() {
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.STRING));
        putRecords("0", 3);
        String invalidCursor = "1234e";
        GetRecordsResult r1 = client.getRecords(projectName, topicName, "0", invalidCursor, 10, schema);
    }

    /**
     * use shard id 0
     */
    @Test (expectedExceptions = MalformedRecordException.class)
    public void testInvalidSchemaType() {
        putRecords("0", 3);
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.BOOLEAN));
        GetCursorResult c1 = client.getCursor(projectName,topicName,"0", GetCursorRequest.CursorType.OLDEST);
        GetRecordsResult r1 = client.getRecords(projectName, topicName, "0", c1.getCursor(), 10, schema);
    }

    /**
     * use shard id 0
     */
    @Test
    public void testInvalidFieldCount() {
        putRecords("0", 3);
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.STRING));
        schema.addField(new Field("test2", FieldType.STRING));
        schema.addField(new Field("test3", FieldType.STRING));
        GetCursorResult c1 = client.getCursor(projectName,topicName,"0", GetCursorRequest.CursorType.OLDEST);
        GetRecordsResult r1 = client.getRecords(projectName, topicName, "0", c1.getCursor(), 10, schema);
        Assert.assertEquals(r1.getRecordCount(), 3);
        for (RecordEntry r : r1.getRecords()) {
            Assert.assertEquals(r.getString("test3"), null);
        }
    }

    /**
     * use shard id 0
     */
    @Test
    public void testInvalidFieldCount2() {
        putRecords("0", 3);
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.STRING));
        GetCursorResult c1 = client.getCursor(projectName,topicName,"0", GetCursorRequest.CursorType.OLDEST);
        GetRecordsResult r1 = client.getRecords(projectName, topicName, "0", c1.getCursor(), 10, schema);
        Assert.assertEquals(r1.getRecordCount(), 3);
        for (RecordEntry r : r1.getRecords()) {
            Assert.assertNotEquals(r.getString("test"), null);
        }
    }

    /**
     * use shard id 1
     */
    @Test
    public void testGetRecordsWithEmptyShardByC1C2C3C4() {
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.STRING));
        String validCursor = "30000000000000000000000000000000";
        GetRecordsResult r1 = client.getRecords(projectName, topicName, "1", validCursor, 10, schema);
        Assert.assertEquals(r1.getNextCursor(), validCursor);
    }


    /**
     * use shard id 1
     */
    @Test (expectedExceptions = InvalidCursorException.class)
    public void testGetRecordsWithEmptyShardByInvalidCursor() {
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.STRING));
        String invalidCursor = "0000000000000011";
        GetRecordsResult r2 = client.getRecords(projectName, topicName, "1", invalidCursor, 10, schema);
        System.out.println(r2.getRecordCount());
        System.out.println(r2.getNextCursor());
    }

    /**
     * use shard id 2
     */
    @Test
    public void testGetRecordsWithBeginCursor() {
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.STRING));

        GetCursorResult c1 = client.getCursor(projectName,topicName,"2",System.currentTimeMillis()-7*24*3600*1000);
        GetRecordsResult r1 = client.getRecords(projectName, topicName, "2", c1.getCursor(), 10, schema);
        Assert.assertEquals(r1.getRecordCount(), 0);
        Assert.assertEquals(r1.getNextCursor(), c1.getCursor());

        putRecords("2", 3);
        GetRecordsResult r2 = client.getRecords(projectName, topicName, "2", c1.getCursor(), 10, schema);
        Assert.assertEquals(r2.getRecordCount(), 3);
    }

    /**
     * use shard id 2
     */
    @Test (expectedExceptions = ResourceNotFoundException.class)
    public void testGetRecordsWithNotExistProject() {
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.STRING));

        String c = new String("30000000000000000000000000000000");
        GetRecordsResult r1 = client.getRecords("not_exist_project", topicName, "2", c, 10, schema);
    }

    /**
     * use shard id 2
     */
    @Test (expectedExceptions = ResourceNotFoundException.class)
    public void testGetRecordsWithNotExistTopic() {
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.STRING));

        String c = new String("30000000000000000000000000000000");
        GetRecordsResult r1 = client.getRecords(projectName, "not_eixts_topic", "2", c, 10, schema);
    }

    /**
     * use shard id 2
     */
    @Test (expectedExceptions = ResourceNotFoundException.class)
    public void testGetRecordsWithNotExistShard() {
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.STRING));

        String c = new String("30000000000000000000000000000000");
        GetRecordsResult r1 = client.getRecords(projectName, topicName, "not_exist_shard", c, 10, schema);
    }

    @Test
    public void testGetRecordsWithNullField() {
        String topicName = DatahubTestUtils.getRandomTopicName();
        int shardCount = 3;
        int lifeCycle = 3;
        RecordType type = RecordType.TUPLE;
        RecordSchema schema = DatahubTestUtils.createSchema("string a, bigint b, double c, timestamp d, boolean e");
        String comment = "";
        Topic topic = this.project.createTopic(topicName, shardCount, lifeCycle, type, schema, comment);
        topic.waitForShardReady();

        RecordEntry entry = new RecordEntry(schema);
        entry.setShardId("0");
        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
        recordEntries.add(entry);

        recordEntries.get(0).setString("a", DatahubTestUtils.getRandomString());
        recordEntries.get(0).setBigint("b", null);
        recordEntries.get(0).setDouble("c", DatahubTestUtils.getRandomDecimal());
        recordEntries.get(0).setTimeStamp("d", DatahubTestUtils.getRandomTimestamp());
        recordEntries.get(0).setBoolean("e", DatahubTestUtils.getRandomBoolean());

        PutRecordsResult result = topic.putRecords(recordEntries);
        Assert.assertEquals(0, result.getFailedRecordCount());

        String c = new String("30000000000000000000000000000000");
        GetRecordsResult records = topic.getRecords("0", c, 1);
        Assert.assertEquals(records.getRecords().get(0).getString(0), entry.getString(0));
        Assert.assertNull(records.getRecords().get(0).getBigint(1));
        Assert.assertEquals(records.getRecords().get(0).getDouble(2), entry.getDouble(2));
        Assert.assertEquals(records.getRecords().get(0).getTimeStamp(3), entry.getTimeStamp(3));
        Assert.assertEquals(records.getRecords().get(0).getBoolean(4), entry.getBoolean(4));
        project.deleteTopic(topicName);
    }

    private void testRecordIntField(List<RecordEntry> rs1, List<RecordEntry> rs2, int index) {
        Assert.assertEquals(rs1.size(), rs2.size());
        for (int i = 0; i < rs1.size(); ++i) {
            Assert.assertEquals(rs1.get(i).getBigint(index), rs2.get(i).getBigint(index));
        }
    }
/*
    @Test
    public void testGetRecordsWithNextCursor() {
        String topicName = DatahubTestUtils.getRandomTopicName();
        int shardCount = 3;
        int lifeCycle = 3;
        RecordType type = RecordType.TUPLE;
        RecordSchema schema = DatahubTestUtils.createSchema("bigint b");
        String comment = "";
        Topic topic = this.project.createTopic(topicName, shardCount, lifeCycle, type, schema, comment);
        topic.waitForShardReady();

        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
        for (int i = 0; i < 100; ++i) {
            RecordEntry entry = new RecordEntry(schema);
            entry.setShardId("0");
            entry.setBigint(0, i + 1L);
            recordEntries.add(entry);
        }

        Assert.assertEquals(0, topic.putRecords(recordEntries).getFailedRecordCount());

        String cursor = topic.getCursor("0", GetCursorRequest.CursorType.OLDEST);

        GetRecordsResult getRecordsResult = topic.getRecords("0", cursor, 100);

        Assert.assertEquals(100, getRecordsResult.getRecordCount());

        GetRecordsResult emptyResult = topic.getRecords("0", getRecordsResult.getNextCursor(), 100);

        Assert.assertEquals(0, emptyResult.getRecordCount());

        emptyResult = topic.getRecords("0", getRecordsResult.getRecords().get(99).getNextCursor(), 100);

        Assert.assertEquals(0, emptyResult.getRecordCount());

        testRecordIntField(recordEntries, getRecordsResult.getRecords(), 0);

        getRecordsResult = topic.getRecords("0", getRecordsResult.getRecords().get(4).getNextCursor(), 10);

        testRecordIntField(recordEntries.subList(5, 15), getRecordsResult.getRecords(), 0);

        String resultNextCursor = getRecordsResult.getNextCursor();
        String recordNextCursor = getRecordsResult.getRecords().get(9).getNextCursor();

        testRecordIntField(recordEntries.subList(15, 25), topic.getRecords("0", resultNextCursor, 10).getRecords(), 0);
        testRecordIntField(recordEntries.subList(15, 25), topic.getRecords("0", recordNextCursor, 10).getRecords(), 0);
    }
*/
}
