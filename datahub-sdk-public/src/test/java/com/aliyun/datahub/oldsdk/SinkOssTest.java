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

/**
 * Created by wz on 17/4/14.
 */
@Test
public class SinkOssTest extends ConnectorTest {
    private String projectName;
    private String topicName;

    private OssDesc config;
    private String ossEndpoint;
    private String bucket;
    private String ossAccessId;
    private String ossAccessKey;

    private String timeFormat;
    private int timeRange;

    @BeforeTest
    void beforeTest() {
        super.SetUp();
        projectName = DatahubTestUtils.getProjectName();
        connectorType = ConnectorType.SINK_OSS;

        ossEndpoint = DatahubTestUtils.getOssEndpoint();
        bucket = DatahubTestUtils.getOssBucket();
        ossAccessId = DatahubTestUtils.getOssAccessId();
        ossAccessKey = DatahubTestUtils.getOssAccessKey();
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

    private void createTupleTopic(String topicName, RecordSchema schema) {
        client.createTopic(projectName, topicName, 3, 1, RecordType.TUPLE, schema, "");
        client.waitForShardReady(projectName, topicName);
    }

    private void createBlobTopic(String topicName) {
        client.createTopic(projectName, topicName, 3, 1, RecordType.BLOB, "");
        client.waitForShardReady(projectName, topicName);
    }

    private RecordSchema genSchema() {
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("f1", FieldType.STRING));
        schema.addField(new Field("f2", FieldType.BIGINT));
        schema.addField(new Field("f3", FieldType.DOUBLE));
        schema.addField(new Field("f4", FieldType.TIMESTAMP));
        schema.addField(new Field("f5", FieldType.BOOLEAN));
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
            config = new OssDesc();
        }
        config.setEndpoint(ossEndpoint);
        config.setBucket(bucket);
        config.setPrefix(projectName + "/" + topicName);
        config.setTimeFormat("%Y%m%d%H%M");
        config.setTimeRange(5);
        config.setAuthMode("ak");
        config.setAccessId(ossAccessId);
        config.setAccessKey(ossAccessKey);
        return config;
    }

    private List<RecordEntry> genTupleRecords(int count, RecordSchema schema, String expect) {
        return null;
    }

    private List<BlobRecordEntry> genBlobRecords(int count, String expect) {
        List<BlobRecordEntry> recordEntries = new ArrayList<BlobRecordEntry>();
        for (int i = 0; i < count ; i++) {
            BlobRecordEntry recordEntry = new BlobRecordEntry();
            recordEntry.setData(expect.getBytes());
            recordEntry.setShardId("0");
            recordEntries.add(recordEntry);
        }
        return recordEntries;
    }

    private void putTupleAndCheck(RecordSchema schema) {
        String expect = "";
        List<RecordEntry> records = genTupleRecords(10, schema, expect);
        client.putRecords(projectName, topicName, records);

        client.createDataConnector(projectName, topicName, connectorType, getColumnFields(schema),getConnectorConfig());

        String actual = "";
        Assert.assertEquals(expect, actual);
    }

    private void putBlobRecords() {
        String expect = "";
        List<BlobRecordEntry> records = genBlobRecords(10, expect);
        client.putBlobRecords(projectName, topicName, records);

        String actual = "";
        Assert.assertEquals(expect, actual);
    }

    private void checkTupleRecords() {

    }

    private void checkBlobRecords() {

    }

    @Test
    void testCreateTupleNormal() {
        RecordSchema schema = genSchema();
        createTupleTopic(topicName, schema);
        client.createDataConnector(projectName, topicName, connectorType, getColumnFields(schema), getConnectorConfig());

        ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
        Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
        Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

        ListDataConnectorResult result = client.listDataConnector(projectName, topicName);
        Assert.assertTrue(result.getDataConnectors().contains("sink_oss"));

        Assert.assertTrue(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
        client.deleteDataConnector(projectName, topicName, connectorType);
        Assert.assertFalse(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));

        result = client.listDataConnector(projectName, topicName);
        Assert.assertTrue(!result.getDataConnectors().contains("sink_oss"));
    }

    @Test
    void testCreateBlobNormal() {
        createBlobTopic(topicName);
        client.createDataConnector(projectName, topicName, connectorType, getConnectorConfig());

        ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
        Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
        Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

        ListDataConnectorResult result = client.listDataConnector(projectName, topicName);
        Assert.assertTrue(result.getDataConnectors().contains("sink_oss"));

        Assert.assertTrue(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
        client.deleteDataConnector(projectName, topicName, connectorType);
        Assert.assertFalse(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));

        result = client.listDataConnector(projectName, topicName);
        Assert.assertTrue(!result.getDataConnectors().contains("sink_oss"));
    }

    @Test
    void testInvalidPrefix() {
        // [0-9, a-z, A-z, _, -]
        createBlobTopic(topicName);
        OssDesc config = (OssDesc)getConnectorConfig();
        config.setPrefix("");

        client.createDataConnector(projectName, topicName, connectorType, config);
        client.deleteDataConnector(projectName, topicName, connectorType);

        config.setPrefix("sdf\\sdf");
        try {
            client.createDataConnector(projectName, topicName, connectorType, config);
            Assert.assertTrue(false);
        } catch (InvalidParameterException e) {
            Assert.assertTrue(e.getErrorMessage().toLowerCase().contains("prefix"));
        }

        config.setPrefix("%*!#");
        try {
            client.createDataConnector(projectName, topicName, connectorType, config);
            Assert.assertTrue(false);
        } catch (InvalidParameterException e) {
            Assert.assertTrue(e.getErrorMessage().toLowerCase().contains("prefix"));
        }
    }

    @Test
    void testInvalidTimeFormat() {

        createBlobTopic(topicName);

        OssDesc config = (OssDesc)getConnectorConfig();
        config.setTimeFormat("%Y%m");
        config.setTimeRange(24 * 60);

        try {
            client.createDataConnector(projectName, topicName, connectorType, config);
            Assert.assertTrue(false);
        } catch (InvalidParameterException e) {
            Assert.assertTrue(e.getErrorMessage().toLowerCase().contains("timeformat"));
        }

        config.setTimeFormat("%Y%m%d");
        config.setTimeRange(23 * 60);
        try {
            client.createDataConnector(projectName, topicName, connectorType, config);
            Assert.assertTrue(false);
        } catch (InvalidParameterException e) {
            Assert.assertTrue(e.getErrorMessage().toLowerCase().contains("timeformat"));
        }

        config.setTimeFormat("%Y%m%d%H");
        config.setTimeRange(5);
        try {
            client.createDataConnector(projectName, topicName, connectorType, config);
            Assert.assertTrue(false);
        } catch (InvalidParameterException e) {
            Assert.assertTrue(e.getErrorMessage().toLowerCase().contains("timeformat"));
        }

        config.setTimeFormat("%Y%m%d%M");
        config.setTimeRange(5);
        try {
            client.createDataConnector(projectName, topicName, connectorType, config);
            Assert.assertTrue(false);
        } catch (InvalidParameterException e) {
            Assert.assertTrue(e.getErrorMessage().toLowerCase().contains("timeformat"));
        }

        config.setTimeFormat("%Y%m%d");
        config.setTimeRange(24 * 60);
        client.createDataConnector(projectName, topicName, connectorType, config);
        client.deleteDataConnector(projectName, topicName, connectorType);

        config.setTimeFormat("%Y%m%d%H");
        config.setTimeRange(60);
        client.createDataConnector(projectName, topicName, connectorType, config);
        client.deleteDataConnector(projectName, topicName, connectorType);

        config.setTimeFormat("%Y%m%d%H%M");
        config.setTimeRange(59);
        client.createDataConnector(projectName, topicName, connectorType, config);
        client.deleteDataConnector(projectName, topicName, connectorType);
    }

    @Test
    void testInvalidTimeRange() {

        createBlobTopic(topicName);
        OssDesc config = (OssDesc)getConnectorConfig();
        config.setTimeRange(1000*24*60 + 1);
        try {
            client.createDataConnector(projectName, topicName, connectorType, config);
            Assert.assertTrue(false);
        } catch (InvalidParameterException e) {
            Assert.assertTrue(e.getErrorMessage().toLowerCase().contains("timerange"));
        }

        config.setTimeRange(0);
        try {
            client.createDataConnector(projectName, topicName, connectorType, config);
            Assert.assertTrue(false);
        } catch (InvalidParameterException e) {
            Assert.assertTrue(e.getErrorMessage().toLowerCase().contains("timerange"));
        }

        config.setTimeRange(-1);
        try {
            client.createDataConnector(projectName, topicName, connectorType, config);
        } catch (InvalidParameterException e) {
            Assert.assertTrue(e.getErrorMessage().toLowerCase().contains("timerange"));
        }
    }

    @Test
    void testSinkTupleNormal() {
//        RecordSchema schema = genSchema();
//        createTupleTopic(topicName, schema);
//        client.createDataConnector(projectName, topicName, connectorType, getConnectorConfig());
//
//        putTupleAndCheck(schema);
    }

    @Test
    void testSinkBlobNormal() {
//        RecordSchema schema = genSchema();
//        createTupleTopic(topicName, schema);
//        client.createDataConnector(projectName, topicName, connectorType, getConnectorConfig());
//
//        putBlobRecords();
//        checkBlobRecords();
    }

    @Test
    void testSinkTupleWithNull() {

    }
}
