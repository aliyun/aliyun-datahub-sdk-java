package com.aliyun.datahub.client.e2e;

import com.aliyun.datahub.client.e2e.common.Configure;
import com.aliyun.datahub.client.e2e.common.Constant;
import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.model.*;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.fail;

public class SinkFcTest extends BaseConnectorTest {
    private SinkFcConfig config;

    @BeforeMethod
    @Override
    public void setUpBeforeMethod() {
        super.setUpBeforeMethod();
        connectorType = ConnectorType.SINK_FC;
        config = new SinkFcConfig() {{
            setAuthMode(AuthMode.AK);
            setEndpoint(Configure.getString(Constant.FC_ENDPOINT));
            setService(Configure.getString(Constant.FC_SERVICE));
            setFunction(Configure.getString(Constant.FC_FUNCTION));
            setAccessId(Configure.getString(Constant.FC_ACCESS_ID));
            setAccessKey(Configure.getString(Constant.FC_ACCESS_KEY));
        }};
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

                client.deleteTopic(TEST_PROJECT_NAME, topic);
            }

        } catch (Exception e) {
            //
        }
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
                        recordData.setField(fieldName, "test\"test");
                        break;
                    case BOOLEAN:
                        recordData.setField(fieldName, random.nextBoolean());
                        break;
                    case DOUBLE:
                        recordData.setField(fieldName, random.nextInt(1000) / 100.0);
                        break;
                    case TIMESTAMP:
                        recordData.setField(fieldName, System.currentTimeMillis() * 1000); // us
                        break;
                    case BIGINT:
                        recordData.setField(fieldName, random.nextLong());
                        break;
                    case DECIMAL:
                        recordData.setField(fieldName, new BigDecimal(random.nextDouble()));
                        break;
                }
            }
        }

        RecordEntry recordEntry = new RecordEntry() {{ setRecordData(recordData); }};
        recordEntry.setPartitionKey(String.valueOf(random.nextInt(RECORD_COUNT)));
        return recordEntry;
    }

    @Test
    public void testSinkTupleNormal() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.BIGINT));
            addField(new Field("f3", FieldType.DOUBLE));
        }};

        createTupleTopicBySchema(schema);

        List<String> columnFields = Arrays.asList("f1", "f2", "f3");
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);

        sleepInMs(MAX_OPERATOR_INTERVAL);
        // get connector
        GetConnectorResult getConnectorResult = client.getConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType);
        SinkFcConfig getConfig = (SinkFcConfig)getConnectorResult.getConfig();
        Assert.assertEquals(getConfig.getEndpoint(), config.getEndpoint());
        Assert.assertEquals(getConfig.getFunction(), config.getFunction());

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

        // split shard
        client.splitShard(TEST_PROJECT_NAME, tupleTopicName, "0");

        sleepInMs(MAX_OPERATOR_INTERVAL);
        // start connector
        client.updateConnectorState(TEST_PROJECT_NAME, tupleTopicName, connectorType, ConnectorState.RUNNING);

        // wait reload
        sleepInMs(MAX_OPERATOR_INTERVAL);

        // write data to new shard
        client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);

        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        // check sealed state
        getConnectorShardStatusResult = client.getConnectorShardStatus(TEST_PROJECT_NAME, tupleTopicName, connectorType);
        for (Map.Entry<String, ConnectorShardStatusEntry> entrySet : getConnectorShardStatusResult.getStatusEntryMap().entrySet()) {
            if (entrySet.getKey().equals("0")) {
                Assert.assertEquals(entrySet.getValue().getState(), ConnectorShardState.FINISHED);
            } else {
                Assert.assertEquals(entrySet.getValue().getState(), ConnectorShardState.EXECUTING);
            }
        }
    }

    @Test
    public void testSinkTupleAppendField() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.BIGINT));
            addField(new Field("f3", FieldType.DOUBLE));
        }};

        createTupleTopicBySchema(schema);

        List<String> columnFields = Arrays.asList("f1", "f2", "f3");
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);

        // put records
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < RECORD_COUNT; ++i) {
            recordEntries.add(genData(schema, null));
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);

        client.appendField(TEST_PROJECT_NAME, tupleTopicName, new Field("f4", FieldType.BIGINT));
        client.appendConnectorField(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_FC, "f4");

        GetConnectorResult getConnectorResult = client.getConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType);
        Assert.assertEquals(getConnectorResult.getColumnFields().contains("f4"), true);

        // put records after append col
        recordEntries.clear();
        schema.addField(new Field("f4", FieldType.BIGINT));
        for (int i = 0; i < RECORD_COUNT; ++i) {
            recordEntries.add(genData(schema, null));
        }

        putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);

        // wait for sink
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        long total = 0L;
        GetConnectorShardStatusResult getConnectorShardStatusResult = client.getConnectorShardStatus(TEST_PROJECT_NAME, tupleTopicName, connectorType);
        for (Map.Entry<String, ConnectorShardStatusEntry> entrySet : getConnectorShardStatusResult.getStatusEntryMap().entrySet()) {
            Assert.assertEquals(entrySet.getValue().getState(), ConnectorShardState.EXECUTING);
            Assert.assertEquals(entrySet.getValue().getDiscardCount(), 0);
            total += entrySet.getValue().getCurrSequence() + 1;
        }
        Assert.assertEquals(total, RECORD_COUNT*2);
    }

    @Test
    public void testSinkWithEndPointEmpty() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.BIGINT));
            addField(new Field("f3", FieldType.DOUBLE));
        }};

        createTupleTopicBySchema(schema);

        List<String> columnFields = Arrays.asList("f1", "f2", "f3");
        config.setEndpoint(null);
        try {
            client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);
            fail("Create connector with null endpoint should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }


        config.setEndpoint("");
        try {
            client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);
            fail("Create connector with empty endpoint should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }
}
