package com.aliyun.datahub;

import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.*;
import com.aliyun.datahub.util.DatahubTestUtils;
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
@Test
public class SinkFcTest {
    private DatahubClient client;
    private String projectName;
    private String topicName;
    private ConnectorType type;

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
        client = new DatahubClient(DatahubTestUtils.getConf());
        projectName = DatahubTestUtils.getProjectName();
        type = ConnectorType.SINK_FC;

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
            client.deleteDataConnector(projectName, topicName, ConnectorType.SINK_FC);
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
        client.createDataConnector(projectName, topicName, type, getConnectorConfig());
        ListDataConnectorResult result = client.listDataConnector(projectName, topicName);
        Assert.assertTrue(result.getDataConnectors().contains("sink_fc"));

        List<BlobRecordEntry> recordEntries = genBlobRecords(20, "test records");
        client.putBlobRecords(projectName, topicName, recordEntries);

        client.deleteDataConnector(projectName, topicName, type);
        result = client.listDataConnector(projectName, topicName);
        Assert.assertTrue(!result.getDataConnectors().contains("sink_fc"));
    }
}
