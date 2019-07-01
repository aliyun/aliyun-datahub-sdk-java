package com.aliyun.datahub.client.e2e;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.e2e.common.Configure;
import com.aliyun.datahub.client.e2e.common.Constant;
import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.model.*;
import com.google.common.base.Charsets;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.fail;

public class SinkDatahubTest extends BaseConnectorTest {
    private SinkDatahubConfig config;
    private String sinkProject;
    private String sinkTopic;
    private int sinkShardCount;
    private DatahubClient sinkClient;
    private static final long TIMESTAMP = System.currentTimeMillis() * 1000;

    @BeforeMethod
    @Override
    public void setUpBeforeMethod() {
        super.setUpBeforeMethod();
        sinkShardCount = SHARD_COUNT;
        sinkProject = Configure.getString(Constant.SINK_DATAHUB_PROJECT);
        sinkTopic = "sink_topic_" + tupleTopicName;
        connectorType = ConnectorType.SINK_DATAHUB;
        config = new SinkDatahubConfig() {{
            setAuthMode(AuthMode.AK);
            setEndpoint(Configure.getString(Constant.SINK_DATAHUB_ENDPOINT));
            setAccessId(Configure.getString(Constant.SINK_DATAHUB_ACCESS_ID));
            setAccessKey(Configure.getString(Constant.SINK_DATAHUB_ACCESS_KEY));
            setProjectName(sinkProject);
            setTopicName(sinkTopic);
        }};

        sinkClient = DatahubClientBuilder.newBuilder().setDatahubConfig(
                new DatahubConfig(
                        config.getEndpoint(),
                        new AliyunAccount(config.getAccessId(), config.getAccessKey()), true
                )
        ).build();
    }


    @AfterMethod
    @Override
    public void tearDownAfterMethod() {
        super.tearDownAfterMethod();
        try {
            ListTopicResult listTopicResult = client.listTopic(TEST_PROJECT_NAME);
            for (String topic : listTopicResult.getTopicNames()) {
                ListConnectorResult listConnectorResult = client.listConnector(TEST_PROJECT_NAME, topic);
                for (String connector : listConnectorResult.getConnectorNames()) {
                    client.deleteConnector(TEST_PROJECT_NAME, topic, ConnectorType.valueOf(connector.toUpperCase()));
                }

                ListSubscriptionResult listSubscriptionResult = client.listSubscription(TEST_PROJECT_NAME, topic, 1, 100);
                for (SubscriptionEntry subEntry : listSubscriptionResult.getSubscriptions()) {
                    client.deleteSubscription(TEST_PROJECT_NAME, topic, subEntry.getSubId());
                }

                client.deleteTopic(TEST_PROJECT_NAME, topic);
            }

            sinkClient.deleteTopic(sinkProject, sinkTopic);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void createSinkTupleTopic(RecordSchema schema) {
        sinkClient.createTopic(sinkProject, sinkTopic, sinkShardCount, 1, RecordType.TUPLE, schema, "test tuple topic");
        sinkClient.waitForShardReady(sinkProject, sinkTopic);
    }

    private void createSinkBlobTopic() {
        sinkClient.createTopic(sinkProject, sinkTopic, sinkShardCount, 1, RecordType.BLOB, "test blob topic");
        sinkClient.waitForShardReady(sinkProject, sinkTopic);
    }

    private RecordEntry genBlobData() {
        final BlobRecordData recordData = new BlobRecordData("BLOB_TEST".getBytes(Charsets.UTF_8));
        RecordEntry recordEntry = new RecordEntry() {{ setRecordData(recordData); }};
        recordEntry.setPartitionKey(String.valueOf(random.nextInt(RECORD_COUNT)));
        return recordEntry;
    }

    private RecordEntry genData(RecordSchema schema, List<String> ignoreList) {
        final TupleRecordData recordData = new TupleRecordData(schema);
        for (Field field : schema.getFields()) {
            String fieldName = field.getName();
            if (ignoreList != null && ignoreList.contains(fieldName)) {
                recordData.setField(fieldName, null);
            } else {
                switch (field.getType()) {
                    case STRING:
                        // set null
                        // recordData.setField(fieldName, String.valueOf(TIMESTAMP));
                        break;
                    case BOOLEAN:
                        recordData.setField(fieldName, false);
                        break;
                    case DOUBLE:
                        recordData.setField(fieldName, (double)TIMESTAMP);
                        break;
                    case TIMESTAMP:
                        recordData.setField(fieldName, TIMESTAMP); // us
                        break;
                    case BIGINT:
                        recordData.setField(fieldName, TIMESTAMP);
                        break;
                    case DECIMAL:
                        recordData.setField(fieldName, BigDecimal.valueOf(Math.sqrt(TIMESTAMP)));
                        break;
                }
            }
        }

        RecordEntry recordEntry = new RecordEntry() {{ setRecordData(recordData); }};
        recordEntry.setPartitionKey(String.valueOf(random.nextInt(RECORD_COUNT)));
        return recordEntry;
    }

    private void checkSinkedData(RecordSchema sinkSchema, int recordCount) {
        for (int i = 0; i < SHARD_COUNT; ++i) {
            String shardId = String.valueOf(i);
            GetCursorResult getCursorResult = sinkClient.getCursor(sinkProject, sinkTopic, shardId, CursorType.OLDEST);
            GetRecordsResult getRecordsResult = sinkClient.getRecords(sinkProject, sinkTopic, shardId, sinkSchema, getCursorResult.getCursor(), recordCount);
            for (RecordEntry entry : getRecordsResult.getRecords()) {
                if (entry.getRecordData() instanceof TupleRecordData) {
                    checkTupleData(sinkSchema, (TupleRecordData) entry.getRecordData());
                } else {
                    BlobRecordData data = (BlobRecordData)entry.getRecordData();
                    Assert.assertEquals(new String(data.getData(), Charsets.UTF_8), "BLOB_TEST");
                }
            }
        }
    }

    private void checkTupleData(RecordSchema schema, TupleRecordData data) {
        for (Field field : schema.getFields()) {
            switch (field.getType()) {
                case BIGINT:
                    Assert.assertEquals(data.getField(field.getName()), TIMESTAMP);
                    break;
                case BOOLEAN:
                    Assert.assertEquals(data.getField(field.getName()), false);
                    break;
                case STRING:
                    Assert.assertNull(data.getField(field.getName()));
//                    Assert.assertEquals(data.getField(field.getName()), String.valueOf(TIMESTAMP));
                    break;
                case DOUBLE:
                    Assert.assertEquals(data.getField(field.getName()), (double)TIMESTAMP);
                    break;
                case DECIMAL:
                    BigDecimal bd = BigDecimal.valueOf(Math.sqrt(TIMESTAMP));
                    if (bd.scale() > 18) {
                        bd = bd.setScale(18, BigDecimal.ROUND_HALF_UP);
                    }
                    Assert.assertEquals(data.getField(field.getName()), bd);
                    break;
                case TIMESTAMP:
                    break;
            }
        }
    }

    @Test
    public void testSinkTupleNormal() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.DECIMAL));
            addField(new Field("f3", FieldType.BIGINT));
        }};

        createTupleTopicBySchema(schema);
//        RecordSchema sinkSchema = new RecordSchema() {{
//            addField(new Field("f2", FieldType.DECIMAL));
//            addField(
//            new Field("f3", FieldType.BIGINT));
//        }};
        createSinkTupleTopic(schema);

        List<String> columnFields = Arrays.asList("f1", "f2", "f3");
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);

        sleepInMs(MAX_OPERATOR_INTERVAL);

        // get connector
        GetConnectorResult getConnectorResult = client.getConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType);
        SinkDatahubConfig getConfig = (SinkDatahubConfig)getConnectorResult.getConfig();
        Assert.assertEquals(getConfig.getEndpoint(), config.getEndpoint());
        Assert.assertEquals(getConfig.getProjectName(), config.getProjectName());
        Assert.assertEquals(getConnectorResult.getState(), ConnectorState.RUNNING);

        // put records
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < RECORD_COUNT; ++i) {
            recordEntries.add(genData(schema, null));
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);

        // wait for sink
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);
        checkSinkedData(schema, RECORD_COUNT);

        // stop connector
        client.updateConnectorState(TEST_PROJECT_NAME, tupleTopicName, connectorType, ConnectorState.STOPPED);
        sleepInMs(MAX_OPERATOR_INTERVAL);

        getConnectorResult = client.getConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType);
        Assert.assertEquals(getConnectorResult.getState(), ConnectorState.STOPPED);

        GetConnectorShardStatusResult getConnectorShardStatusResult = client.getConnectorShardStatus(TEST_PROJECT_NAME, tupleTopicName, connectorType);
        for (ConnectorShardStatusEntry entry : getConnectorShardStatusResult.getStatusEntryMap().values()) {
            Assert.assertEquals(entry.getState(), ConnectorShardState.STOPPED);
        }
        // update connector offset
        ConnectorOffset offset = new ConnectorOffset() {{ setSequence(10); setTimestamp(1000);}};
        client.updateConnectorOffset(TEST_PROJECT_NAME, tupleTopicName, connectorType, null, offset);

        getConnectorShardStatusResult = client.getConnectorShardStatus(TEST_PROJECT_NAME, tupleTopicName, connectorType);
        for (ConnectorShardStatusEntry entry : getConnectorShardStatusResult.getStatusEntryMap().values()) {
            Assert.assertEquals(entry.getCurrSequence(), 10);
            Assert.assertEquals(entry.getCurrTimestamp(), 1000);
        }
    }

    @Test
    public void testSinkBlobNormal() {
        createBlobTopic();
        createSinkBlobTopic();

        List<String> columnFields = Arrays.asList("f1", "f2", "f3");
        client.createConnector(TEST_PROJECT_NAME, blobTopicName, connectorType, columnFields, config);

        sleepInMs(MAX_OPERATOR_INTERVAL);

        // get connector
        GetConnectorResult getConnectorResult = client.getConnector(TEST_PROJECT_NAME, blobTopicName, connectorType);
        SinkDatahubConfig getConfig = (SinkDatahubConfig)getConnectorResult.getConfig();
        Assert.assertEquals(getConfig.getEndpoint(), config.getEndpoint());
        Assert.assertEquals(getConfig.getProjectName(), config.getProjectName());
        Assert.assertEquals(getConnectorResult.getState(), ConnectorState.RUNNING);

        // put records
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < RECORD_COUNT; ++i) {
            recordEntries.add(genBlobData());
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, blobTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);

        // wait for sink
        waitForAllShardSinked(blobTopicName, MAX_SINK_TIMEOUT);
        checkSinkedData(schema, RECORD_COUNT);

        // stop connector
        client.updateConnectorState(TEST_PROJECT_NAME, blobTopicName, connectorType, ConnectorState.STOPPED);
        sleepInMs(MAX_OPERATOR_INTERVAL);

        getConnectorResult = client.getConnector(TEST_PROJECT_NAME, blobTopicName, connectorType);
        Assert.assertEquals(getConnectorResult.getState(), ConnectorState.STOPPED);

        GetConnectorShardStatusResult getConnectorShardStatusResult = client.getConnectorShardStatus(TEST_PROJECT_NAME, blobTopicName, connectorType);
        for (ConnectorShardStatusEntry entry : getConnectorShardStatusResult.getStatusEntryMap().values()) {
            Assert.assertEquals(entry.getState(), ConnectorShardState.STOPPED);
        }
        // update connector offset
        ConnectorOffset offset = new ConnectorOffset() {{ setSequence(10); setTimestamp(1000);}};
        client.updateConnectorOffset(TEST_PROJECT_NAME, blobTopicName, connectorType, null, offset);

        getConnectorShardStatusResult = client.getConnectorShardStatus(TEST_PROJECT_NAME, blobTopicName, connectorType);
        for (ConnectorShardStatusEntry entry : getConnectorShardStatusResult.getStatusEntryMap().values()) {
            Assert.assertEquals(entry.getCurrSequence(), 10);
            Assert.assertEquals(entry.getCurrTimestamp(), 1000);
        }
    }

    @Test
    public void testSinkWithShardCountNotMatch() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.DECIMAL));
            addField(new Field("f3", FieldType.BIGINT));
        }};

        createTupleTopicBySchema(schema);
        sinkShardCount += 1;
        createSinkTupleTopic(schema);

        List<String> columnFields = Arrays.asList("f1", "f2", "f3");
        try {
            client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);
            fail("SinkWithShardCountNotMatch should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testSinkWithTopicTypeNotMatch() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.DECIMAL));
            addField(new Field("f3", FieldType.BIGINT));
        }};

        createTupleTopicBySchema(schema);
        createSinkBlobTopic();

        List<String> columnFields = Arrays.asList("f1", "f2", "f3");
        try {
            client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);
            fail("SinkWithTopicTypeNotMatch should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testSinkWithSchemaNotMach() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.DECIMAL));
            addField(new Field("f3", FieldType.BIGINT));
        }};

        createTupleTopicBySchema(schema);

        RecordSchema sinkSchema = new RecordSchema() {{
            addField(new Field("f2", FieldType.DECIMAL));
            addField(new Field("f3", FieldType.BIGINT));
        }};
        createSinkTupleTopic(sinkSchema);

        List<String> columnFields = Arrays.asList("f1", "f2", "f3");
        try {
            client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);
            fail("SinkWithTopicTypeNotMatch should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }
}
