package com.aliyun.datahub.oldsdk;

import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.InvalidParameterException;
import com.aliyun.datahub.model.*;
import com.aliyun.datahub.oldsdk.util.ConnectorTest;
import com.aliyun.datahub.oldsdk.util.DatahubTestUtils;
import org.apache.commons.lang3.RandomUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Test(enabled = false)
public class SinkDatahubTest extends ConnectorTest {
    private String fromTopic;
    private String toTopic;
    private String project = DatahubTestUtils.getProjectName();

    @BeforeMethod
    public void SetUp() {
        super.SetUp();
        fromTopic = null;
        toTopic = null;
        connectorType = ConnectorType.SINK_DATAHUB;
    }

    @AfterMethod
    public void TearDown() {
        try {
            client.deleteDataConnector(DatahubTestUtils.getProjectName(), fromTopic, connectorType);
        } catch (DatahubClientException e) {

        }
        if (fromTopic != null) {
            client.deleteTopic(DatahubTestUtils.getProjectName(), fromTopic);
        }

        if (toTopic != null) {
            client.deleteTopic(DatahubTestUtils.getProjectName(), toTopic);
        }
    }

    @Test(enabled = false)
    public void testSinkDatahub() throws InterruptedException {
        RecordSchema schema = DatahubTestUtils.createSchema("bigint a, double b, boolean c, timestamp d, string e");
        fromTopic = DatahubTestUtils.getRandomTopicName();
        client.createTopic(project, fromTopic, 5, 1, RecordType.TUPLE, schema, "from topic");
        client.waitForShardReady(project, fromTopic);
        toTopic = DatahubTestUtils.getRandomTopicName();
        client.createTopic(project, toTopic, 5, 1, RecordType.TUPLE, schema, "from topic");
        client.waitForShardReady(project, toTopic);
        List<String> columns = Arrays.asList("a,b,c,d,e".split(","));
        DatahubDesc config = new DatahubDesc();
        config.setEndpoint(DatahubTestUtils.getConf().getEndpoint());
        config.setProject(project);
        config.setTopic(toTopic);

        client.createDataConnector(project, fromTopic, connectorType, columns, config);

        System.out.println("from topic:" + fromTopic + " to topic:" + toTopic);

            for (int shard = 0 ; shard < 5; ++shard) {
                String cursor = client.getCursor(project, toTopic, String.valueOf(shard), GetCursorRequest.CursorType.LATEST).getCursor();
                for (int j = 0; j < 2; ++j) {
                    int n = RandomUtils.nextInt(1, 1000);
                    List<RecordEntry> records = new ArrayList<RecordEntry>();
                    for (int k = 0; k < n; ++k) {
                        RecordEntry record = DatahubTestUtils.makeRecord(schema, true);
                        record.setShardId(String.valueOf(shard));
                        record.putAttribute(DatahubTestUtils.getRandomString(), DatahubTestUtils.getRandomString());
                        records.add(record);
                    }
                    PutRecordsResult putRecordsResult = client.putRecords(project, fromTopic, records);
                    Assert.assertEquals(0, putRecordsResult.getFailedRecordCount());

                    System.out.println("shard:" + shard + " pack:" + j + " size:" + records.size());
                    List<RecordEntry> acutalRecords = new ArrayList<RecordEntry>();
                    int retry = 10;
                    while (acutalRecords.size() < records.size() && retry > 0) {
                        GetRecordsResult getRecordsResult = client.getRecords(project, toTopic, String.valueOf(shard), cursor, n, schema);
                        acutalRecords.addAll(getRecordsResult.getRecords());
                        cursor = getRecordsResult.getNextCursor();
                        Thread.sleep(500L);
                        System.out.println("cursor:" + cursor + " read size:" + acutalRecords.size());
                        --retry;
                    }
                    if (retry == 0) {
                        Assert.assertTrue(false, "out of retry");
                    }
                    compareRecords(acutalRecords, records);
                }
        }

        client.deleteDataConnector(project, fromTopic, connectorType);
    }

    @Test(enabled = false)
    public void testSinkDatahubBlob() throws InterruptedException {
        fromTopic = DatahubTestUtils.getRandomTopicName();
        client.createTopic(project, fromTopic, 5, 1, RecordType.BLOB, "from topic");
        client.waitForShardReady(project, fromTopic);
        toTopic = DatahubTestUtils.getRandomTopicName();
        client.createTopic(project, toTopic, 5, 1, RecordType.BLOB, "to topic");
        client.waitForShardReady(project, toTopic);

        DatahubDesc config = new DatahubDesc();
        config.setEndpoint(DatahubTestUtils.getConf().getEndpoint());
        config.setProject(project);
        config.setTopic(toTopic);

        client.createDataConnector(project, fromTopic, connectorType, config);

        for (int shard = 0 ; shard < 5; ++shard) {
            String cursor = client.getCursor(project, toTopic, String.valueOf(shard), GetCursorRequest.CursorType.LATEST).getCursor();
            for (int j = 0; j < 2; ++j) {
                int n = RandomUtils.nextInt(1, 1000);
                List<BlobRecordEntry> records = new ArrayList<BlobRecordEntry>();
                for (int k = 0; k < n; ++k) {
                    BlobRecordEntry record = new BlobRecordEntry();
                    record.setShardId(String.valueOf(shard));
                    record.setData(DatahubTestUtils.getRandomString().getBytes());
                    record.putAttribute(DatahubTestUtils.getRandomString(), DatahubTestUtils.getRandomString());
                    records.add(record);
                }
                PutBlobRecordsResult putBlobRecordsResult = client.putBlobRecords(project, fromTopic, records);
                Assert.assertEquals(0, putBlobRecordsResult.getFailedRecordCount());

                List<BlobRecordEntry> acutalRecords = new ArrayList<BlobRecordEntry>();
                while (acutalRecords.size() < records.size()) {
                    GetBlobRecordsResult getRecordsResult = client.getBlobRecords(project, toTopic, String.valueOf(shard), cursor, n);
                    acutalRecords.addAll(getRecordsResult.getRecords());
                    cursor = getRecordsResult.getNextCursor();
                    Thread.sleep(500L);
                }
                compareBlobRecords(acutalRecords, records);
            }
        }

        client.deleteDataConnector(project, fromTopic, connectorType);
    }

    @Test (enabled = false, expectedExceptions = {InvalidParameterException.class}, expectedExceptionsMessageRegExp = ".*schema not match.*")
    public void testSinkDatahubWithMismatchedSchema() {
        RecordSchema fromSchema = DatahubTestUtils.createSchema("bigint a");
        fromTopic = DatahubTestUtils.getRandomTopicName();
        client.createTopic(project, fromTopic, 1, 1, RecordType.TUPLE, fromSchema, "from topic");
        RecordSchema toSchema = DatahubTestUtils.createSchema("string a");
        toTopic = DatahubTestUtils.getRandomTopicName();
        client.createTopic(project, toTopic, 1, 1, RecordType.TUPLE, toSchema, "from topic");

        DatahubDesc config = new DatahubDesc();
        config.setEndpoint(DatahubTestUtils.getConf().getEndpoint());
        config.setProject(project);
        config.setTopic(toTopic);

        client.createDataConnector(project, fromTopic, connectorType, config);
    }

    @Test (enabled = false, expectedExceptions = {InvalidParameterException.class}, expectedExceptionsMessageRegExp = ".*record type not match.*")
    public void testSinkDatahubWithMismatchedRecordType() {
        RecordSchema schema = DatahubTestUtils.createSchema("bigint a");
        fromTopic = DatahubTestUtils.getRandomTopicName();
        client.createTopic(project, fromTopic, 1, 1, RecordType.TUPLE, schema, "from topic");
        toTopic = DatahubTestUtils.getRandomTopicName();
        client.createTopic(project, toTopic, 1, 1, RecordType.BLOB, "from topic");

        DatahubDesc config = new DatahubDesc();
        config.setEndpoint(DatahubTestUtils.getConf().getEndpoint());
        config.setProject(project);
        config.setTopic(toTopic);

        client.createDataConnector(project, fromTopic, connectorType, config);
    }

    @Test (enabled = false, expectedExceptions = {InvalidParameterException.class}, expectedExceptionsMessageRegExp = ".*shard count not match.*")
    public void testSinkDatahubWithMismatchedShardCount() {
        RecordSchema schema = DatahubTestUtils.createSchema("bigint a");
        fromTopic = DatahubTestUtils.getRandomTopicName();
        client.createTopic(project, fromTopic, 1, 1, RecordType.TUPLE, schema, "from topic");
        toTopic = DatahubTestUtils.getRandomTopicName();
        client.createTopic(project, toTopic, 2, 1, RecordType.TUPLE, schema, "from topic");

        DatahubDesc config = new DatahubDesc();
        config.setEndpoint(DatahubTestUtils.getConf().getEndpoint());
        config.setProject(project);
        config.setTopic(toTopic);

        client.createDataConnector(project, fromTopic, connectorType, config);
    }

    private void compareRecords(List<RecordEntry> actual, List<RecordEntry> expected) {
        Assert.assertEquals(actual.size(), expected.size());
        for (int i = 0; i < actual.size(); ++i) {
            compareRecord(actual.get(i), expected.get(i));
        }
    }

    private void compareBlobRecords(List<BlobRecordEntry> actual, List<BlobRecordEntry> expected) {
        Assert.assertEquals(actual.size(), expected.size());
        for (int i = 0; i < actual.size(); ++i) {
            compareBlobRecord(actual.get(i), expected.get(i));
        }
    }

    private void compareBlobRecord(BlobRecordEntry actual, BlobRecordEntry expected) {
        Assert.assertEquals(actual.getData(), expected.getData());
        Assert.assertEquals(actual.getShardId(), expected.getShardId());
        Assert.assertEquals(actual.getAttributes().keySet(), expected.getAttributes().keySet());
        Assert.assertEquals(actual.getAttributes().values(), expected.getAttributes().values());
    }

    private void compareRecord(RecordEntry actual, RecordEntry expected) {
        Assert.assertEquals(actual.getFieldCount(), expected.getFieldCount());
        Assert.assertEquals(actual.getShardId(), expected.getShardId());
        for (int i = 0; i < actual.getFieldCount(); ++i) {
            Assert.assertEquals(actual.getFields()[i].getName(), expected.getFields()[i].getName());
            Assert.assertEquals(actual.getFields()[i].getNotnull(), expected.getFields()[i].getNotnull());
            Assert.assertEquals(actual.getFields()[i].getType(), expected.getFields()[i].getType());
            boolean notNull = actual.getFields()[i].getNotnull();
            FieldType type = actual.getFields()[i].getType();
            if (!notNull) {
                switch (type) {
                    case BIGINT:
                        Assert.assertEquals(actual.getBigint(i), expected.getBigint(i));
                        break;
                    case STRING:
                        Assert.assertEquals(actual.getString(i), expected.getString(i));
                        break;
                    case DOUBLE:
                        Assert.assertEquals(actual.getDouble(i), expected.getDouble(i));
                        break;
                    case BOOLEAN:
                        Assert.assertEquals(actual.getBoolean(i), expected.getBoolean(i));
                        break;
                    case TIMESTAMP:
                        Assert.assertEquals(actual.getTimeStamp(i), expected.getTimeStamp(i));
                        break;
                }
            }
        }
        Assert.assertEquals(actual.getAttributes().keySet(), expected.getAttributes().keySet());
        Assert.assertEquals(actual.getAttributes().values(), expected.getAttributes().values());
    }
}
