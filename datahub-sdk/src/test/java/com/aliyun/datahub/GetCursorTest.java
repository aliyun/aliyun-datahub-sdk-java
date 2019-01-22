package com.aliyun.datahub;

import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.common.data.*;
import com.aliyun.datahub.exception.InvalidParameterException;
import com.aliyun.datahub.exception.ResourceNotFoundException;
import com.aliyun.datahub.model.*;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.util.DatahubTestUtils;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.ArrayList;
import java.util.List;

@Test
public class GetCursorTest {
    private String projectName = null;
    private String topicName = null;
    private DatahubClient client = null;

    @BeforeMethod
    public void SetUp() {
        try {
            DatahubConfiguration conf = DatahubTestUtils.getConf();
            projectName = DatahubTestUtils.getProjectName();
            client = new DatahubClient(conf);
            this.topicName = DatahubTestUtils.getRandomTopicName();
            int shardCount = 3;
            int lifeCycle = 1;
            RecordType type = RecordType.TUPLE;
            RecordSchema schema = new RecordSchema();
            schema.addField(new Field("test", FieldType.BIGINT));
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
        schema.addField(new Field("test", FieldType.BIGINT));

        for (long n = 0; n < recordNum; n++) {
            List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
            //RecordData
            RecordEntry entry = new RecordEntry(schema);

            for (int i = 0; i < entry.getFieldCount(); i++) {
                entry.setBigint(i, n);
            }

            entry.setShardId(shardId);

            recordEntries.add(entry);
            client.putRecords(projectName, topicName, recordEntries);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {

            }
        }

    }

    /**
     * use shard id 0
     */
    @Test
    public void testGetCursorByT1() {

        long timestamp = System.currentTimeMillis();
        putRecords("0", 3);
        GetCursorResult c1 = client.getCursor(projectName, topicName, "0", timestamp - 7 * 24 * 3600 * 1000);
        GetCursorResult c2 = client.getCursor(projectName, topicName, "0", GetCursorRequest.CursorType.OLDEST);
        GetCursorResult c3 = client.getCursor(projectName, topicName, "0", timestamp - 7 * 12 * 3600 * 1000);
        Assert.assertEquals(c1.getCursor(), c2.getCursor());
        Assert.assertEquals(c1.getRecordTime(), c2.getRecordTime());
        Assert.assertEquals(c1.getSequence(), c2.getSequence());
        Assert.assertEquals(c1.getSequence(), 0L);
        Assert.assertEquals(c3.getCursor(), c2.getCursor());
    }

    /**
     * use shard id 0
     */
    @Test
    public void testGetCursorByT2() {

        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.BIGINT));
        putRecords("0", 3);
        GetCursorResult c1 = client.getCursor(projectName, topicName, "0", GetCursorRequest.CursorType.OLDEST);
        GetRecordsResult r1 = client.getRecords(projectName, topicName, "0", c1.getCursor(), 1, schema);
        long timestamp = r1.getRecords().get(0).getBigint(0);

        GetCursorResult c2 = client.getCursor(projectName, topicName, "0", timestamp + 1);
        GetRecordsResult r2 = client.getRecords(projectName, topicName, "0", c2.getCursor(), 10, schema);

        Assert.assertTrue(r2.getRecordCount() > 1);
    }

    /**
     * use shard id 0
     */
    @Test
    public void testGetCursorByT3() {
        putRecords("0", 3);
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.BIGINT));

        GetCursorResult c1 = client.getCursor(projectName, topicName, "0", GetCursorRequest.CursorType.LATEST);
        GetRecordsResult r1 = client.getRecords(projectName, topicName, "0", c1.getCursor(), 10, schema);
        Assert.assertEquals(r1.getRecordCount(), 1);

        long timestamp = r1.getRecords().get(0).getSystemTime();
        String nc = r1.getNextCursor();

        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
        }
        GetCursorResult c2 = client.getCursor(projectName, topicName, "0", timestamp + 1);

        Assert.assertEquals(c2.getCursor(), nc);
    }

    /**
     * use shard id 0
     */
    @Test
    public void testGetCursorSequence() {

        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("test", FieldType.BIGINT));
        putRecords("0", 3);
        GetCursorResult c1 = client.getCursor(projectName, topicName, "0", GetCursorRequest.CursorType.OLDEST);
        GetRecordsResult r1 = client.getRecords(projectName, topicName, "0", c1.getCursor(), 10, schema);
        long timestamp = r1.getRecords().get(0).getSystemTime();

        Assert.assertEquals(timestamp, c1.getRecordTime());

        GetCursorResult c2 = client.getCursor(projectName, topicName, "0", timestamp);
        GetRecordsResult r2 = client.getRecords(projectName, topicName, "0", c2.getCursor(), 10, schema);

        Assert.assertEquals(r2.getRecordCount(), 3);
    }

    /**
     * use shard id 0
     */
    @Test (expectedExceptions = InvalidParameterException.class)
    public void testGetCursorByT4() {
        long timestamp = System.currentTimeMillis();
        GetCursorResult c1 = client.getCursor(projectName, topicName, "0", timestamp + 10000);
    }

    /**
     * use shard id 1
     */
    @Test
    public void testGetCursorWithEmptyShardByT1T2T3() {
        long timestamp = System.currentTimeMillis();
        GetCursorResult c = client.getCursor(projectName, topicName, "1", timestamp - 7 * 24 * 3600 * 1000);
        String cursor = c.getCursor();
        Assert.assertEquals(cursor, "30000000000000000000000000000000");
    }

    @Test
    public void testGetCursorBySeqInEmpty() {
        GetCursorResult c = client.getCursor(projectName, topicName, "2", GetCursorRequest.CursorType.SEQUENCE, 0);
        String cursor = c.getCursor();
        Assert.assertEquals(cursor, "30000000000000000000000000000000");
    }

    @Test
    public void testGetCursorBySeqInNormal() {
        putRecords("2", 10);
        for (int i = 0; i < 10; i++)
        {
            GetCursorResult c = client.getCursor(projectName, topicName, "2", GetCursorRequest.CursorType.SEQUENCE, i);
            String cursor = c.getCursor();
            Assert.assertEquals(cursor.codePointAt(27), i + '0');
        }
    }

    @Test (expectedExceptions = InvalidParameterException.class)
    public void testGetCursorBySeqOutOfRange() {
        long timestamp = System.currentTimeMillis();
        GetCursorResult c = client.getCursor(projectName, topicName, "2", GetCursorRequest.CursorType.SEQUENCE, 11);
    }

    /**
     * use shard id 1
     */
    @Test (expectedExceptions = InvalidParameterException.class)
    public void testGetCursorWithEmptyShardByT4() {
        long timestamp = System.currentTimeMillis();
        GetCursorResult c1 = client.getCursor(projectName, topicName, "1", timestamp + 10000);
    }

    /**
     * use shard id 0
     */
    @Test (expectedExceptions = ResourceNotFoundException.class)
    public void testGetCursorWithNotExistProject() {
        GetCursorResult c1 = client.getCursor("not_exist_project", topicName, "0", GetCursorRequest.CursorType.LATEST);
    }

    /**
     * use shard id 0
     */
    @Test (expectedExceptions = ResourceNotFoundException.class)
    public void testGetcursorWithNotExistTopic() {
        GetCursorResult c1 = client.getCursor(projectName, "not_exist_topic", "0", GetCursorRequest.CursorType.LATEST);
    }

    /**
     * use shard id 0
     */
    @Test (expectedExceptions = ResourceNotFoundException.class)
    public void testGetcursorWithNotExistShard() {
        GetCursorResult c1 = client.getCursor(projectName, topicName, "not_exist_shard", GetCursorRequest.CursorType.LATEST);
    }
}
