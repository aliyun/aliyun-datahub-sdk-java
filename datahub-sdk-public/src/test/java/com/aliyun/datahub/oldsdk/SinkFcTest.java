package com.aliyun.datahub.oldsdk;

import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.exception.DatahubServiceException;
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
 * Created by wz on 17/6/5.
 */
public class SinkFcTest extends ConnectorTest {
    private String projectName;
    private String topicName;

    private FcDesc config;
    private String endpoint;
    private String service;
    private String function;
    private String invocationRole;
    private int batchSize;
    private String startPosition;
    private Long startTimestamp;
    private String accessId;
    private String accessKey;

    @BeforeTest
    private void beforeTest() {
        super.SetUp();
        projectName = DatahubTestUtils.getProjectName();
        connectorType = ConnectorType.SINK_FC;

        endpoint = "http://1444065814263145.fc.cn-shanghai.aliyuncs.com";
        service = "datahub2fc";
        function = "test_func";
        invocationRole = "";
        batchSize = 10;
        startPosition = "OLDEST";
        accessId = "";
        accessKey = "";
    }

    @BeforeMethod
    private void setup() {
        topicName = DatahubTestUtils.getRamdomTopicName();
        createBlobTopic(topicName);
    }

    @AfterMethod
    private void teardown() {
        try {
            client.deleteDataConnector(projectName, topicName, connectorType);
        } catch (DatahubServiceException e) {
        }
        try {
            client.deleteTopic(projectName, topicName);
        } catch (DatahubServiceException e) {
        }
    }

    private void createBlobTopic(String topicName) {
        client.createTopic(projectName, topicName, 10, 1, RecordType.BLOB, "");
        client.waitForShardReady(projectName, topicName);
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

    private ConnectorConfig getConnectorConfig() {
        if (config == null) {
            config = new FcDesc();
        }
        config.setEndpoint(endpoint);
        config.setService(service);
        config.setFunction(function);
        config.setInvocationRole(invocationRole);
        config.setBatchSize(batchSize);
        config.setStartPosition(startPosition);
        config.setAuthMode("sts");
        config.setAccessId(accessId);
        config.setAccessKey(accessKey);
        return config;
    }

    @Test
    public void testCreateBlobNormal() {
        client.createDataConnector(projectName, topicName, connectorType, getConnectorConfig());

        ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
        Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
        Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

        ListDataConnectorResult result = client.listDataConnector(projectName, topicName);
        Assert.assertTrue(result.getDataConnectors().contains("sink_fc"));

        List<BlobRecordEntry> recordEntries = genBlobRecords(20, "test records");
        client.putBlobRecords(projectName, topicName, recordEntries);

        Assert.assertTrue(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
        client.deleteDataConnector(projectName, topicName, connectorType);
        Assert.assertFalse(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
        result = client.listDataConnector(projectName, topicName);
        Assert.assertTrue(!result.getDataConnectors().contains("sink_fc"));
    }
}
