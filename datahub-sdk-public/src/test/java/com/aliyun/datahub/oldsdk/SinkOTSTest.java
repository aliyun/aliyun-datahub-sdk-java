package com.aliyun.datahub.oldsdk;

import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.exception.InvalidParameterException;
import com.aliyun.datahub.model.*;
import com.aliyun.datahub.oldsdk.util.ConnectorExtInfoJsonDeser;
import com.aliyun.datahub.oldsdk.util.ConnectorTest;
import com.aliyun.datahub.oldsdk.util.DatahubTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class SinkOTSTest extends ConnectorTest {
    private String projectName;
    private String topicName;
    private int shardCount;

    private OtsDesc config;
    private String otsEndpoint;
    private String otsInstance;
    private String otsTable;
    private String otsAccessId;
    private String otsAccessKey;

    @BeforeTest
    void beforeTest() {
        super.SetUp();
        projectName = DatahubTestUtils.getProjectName();
        connectorType = ConnectorType.SINK_OTS;

        otsEndpoint = DatahubTestUtils.getOtsEndpoint();
        otsInstance = DatahubTestUtils.getOtsInstance();
        otsTable = DatahubTestUtils.getOtsTable();
        otsAccessId = DatahubTestUtils.getOtsAccessId();
        otsAccessKey = DatahubTestUtils.getOtsAccessKey();

        shardCount = 3;
    }

    @BeforeMethod
    void beforeMethod() {
        topicName = DatahubTestUtils.getRamdomTopicName();
    }

    @AfterMethod
    void teardown() {
        try {
            client.deleteDataConnector(projectName, topicName, connectorType);
        } catch (DatahubServiceException e) {
        }
        try {
            client.deleteTopic(projectName, topicName);
        } catch (DatahubServiceException e) {
        }
    }

    private void createTopic(String topicName, RecordSchema schema) {
        client.createTopic(projectName, topicName, shardCount, 1, RecordType.TUPLE, schema, "");
        client.waitForShardReady(projectName, topicName);
    }

    private RecordSchema genSchema() {
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("val1", FieldType.STRING));
        schema.addField(new Field("pk3", FieldType.BIGINT));
        schema.addField(new Field("val2", FieldType.STRING));
        schema.addField(new Field("pk1", FieldType.STRING));
        schema.addField(new Field("val3", FieldType.BIGINT));
        schema.addField(new Field("pk2", FieldType.STRING));
        return schema;
    }

    private List<String> getColumnFields(RecordSchema schema) {
        ArrayList<String> fields = new ArrayList<String>();
        for (Field field: schema.getFields()) {
            fields.add(field.getName());
        }
        return fields;
    }

    private ConnectorConfig getConnectorConfig() {
        if (config == null) {
            config = new OtsDesc();
        }
        config.setEndpoint(otsEndpoint);
        config.setInstance(otsInstance);
        config.setAuthMode(AuthMode.AK);
        config.setAccessId(otsAccessId);
        config.setAccessKey(otsAccessKey);
        config.setTable(otsTable);
        return config;
    }

    private void putRecords(RecordSchema schema, int recordCount) {
        for (int i = 0; i < shardCount; i++) {
            List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
            for (int j = 0; j < recordCount; j++) {
                RecordEntry record = new RecordEntry(schema);
                record.setShardId(String.valueOf(i));
                record.setString("val1", "val1_" + i + "_" + j);
                record.setBigint("pk3", (long) i * j);
                record.setString("val2", "val2_" + i + "_" + j);
                record.setString("pk1", "pk1_" + i + "_" + j);
                record.setBigint("val3", System.currentTimeMillis() + i + j);
                record.setString("pk2", "pk2_" + i + "_" + j);
                recordEntries.add(record);
            }
            PutRecordsResult putResult = client.putRecords(projectName, topicName, recordEntries);
            Assert.assertTrue(putResult.getFailedRecordCount() == 0);
        }
    }

    @Test
    void testCreateNormal() {
        RecordSchema schema = genSchema();
        createTopic(topicName, schema);
        client.createDataConnector(projectName, topicName, connectorType,
                getColumnFields(schema), getConnectorConfig());

        ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
        Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
        Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

        ListDataConnectorResult result = client.listDataConnector(projectName, topicName);
        Assert.assertTrue(result.getDataConnectors().contains("sink_ots"));

        GetDataConnectorResult connector = client.getDataConnector(projectName, topicName, ConnectorType.SINK_OTS);
        Assert.assertTrue(connector.getConnectorConfig() instanceof OtsDesc);

        putRecords(schema, 10);
        try {
            Thread.sleep(50 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        connector = client.getDataConnector(projectName, topicName, ConnectorType.SINK_OTS);
        List<ShardContext> shardContexts = connector.getShardContexts();
        Assert.assertTrue(shardContexts.size() == shardCount);
        for (ShardContext context : shardContexts) {
            Assert.assertEquals(context.getCurSequence(), 9);
            Assert.assertEquals(GetCurSequenceBySubscriptionId(projectName, topicName,
                    detailRs.getSubscriptionId(), context.getShardId()), context.getCurSequence());
        }
    }

    @Test
    void testCreateNoMatchSchema() {
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("val1", FieldType.STRING));
        schema.addField(new Field("pk3", FieldType.STRING));
        schema.addField(new Field("val2", FieldType.STRING));
        schema.addField(new Field("pk1", FieldType.STRING));
        schema.addField(new Field("val3", FieldType.BIGINT));
        schema.addField(new Field("pk2", FieldType.STRING));

        createTopic(topicName, schema);
        try {
            client.createDataConnector(projectName, topicName, ConnectorType.SINK_OTS,
                    getColumnFields(schema), getConnectorConfig());
            Assert.assertTrue(false);
        } catch (InvalidParameterException e) {
            Assert.assertTrue(e.getMessage().contains("Column field type not match"));
        }

        schema = new RecordSchema();
        schema.addField(new Field("val1", FieldType.STRING));
        schema.addField(new Field("val2", FieldType.STRING));
        schema.addField(new Field("pk1", FieldType.STRING));
        schema.addField(new Field("val3", FieldType.BIGINT));
        schema.addField(new Field("pk2", FieldType.STRING));
        client.deleteTopic(projectName, topicName);
        createTopic(topicName, schema);

        try {
            client.createDataConnector(projectName, topicName, ConnectorType.SINK_OTS,
                    getColumnFields(schema), getConnectorConfig());
            Assert.assertTrue(false);
        } catch (InvalidParameterException e) {
            Assert.assertTrue(e.getMessage().contains("Field pk3 does not exists in topic"));
        }
    }
}
