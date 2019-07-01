package com.aliyun.datahub.client.e2e;

import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.model.*;
import org.testng.annotations.BeforeClass;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public abstract class BaseConnectorTest extends BaseTest {
    protected static boolean sUseNewExecutor = true;
    protected static final int MAX_OPERATOR_INTERVAL = 5000;
    protected static final long MAX_SINK_TIMEOUT = 5 * 60 * 1000;
    protected static final int RECORD_COUNT = 1000;
    protected ConnectorType connectorType;
    protected Random random = new Random(System.currentTimeMillis());

    @BeforeClass
    public static void setUpBeforeClass() {
        bCreateTopic = false;
        BaseTest.setUpBeforeClass();
    }

    protected void createTupleTopicBySchema(RecordSchema schema) {
        client.createTopic(TEST_PROJECT_NAME, tupleTopicName, SHARD_COUNT, 1, RecordType.TUPLE, schema, "test tuple topic");
        client.waitForShardReady(TEST_PROJECT_NAME, tupleTopicName);
    }

    protected void createBlobTopic() {
        client.createTopic(TEST_PROJECT_NAME, blobTopicName, SHARD_COUNT, 1, RecordType.BLOB, "test blob topic");
        client.waitForShardReady(TEST_PROJECT_NAME, blobTopicName);
    }

    protected void sleepInMs(long timeout) {
        try {
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
            //
        }
    }

    protected void waitForAllShardSinked(String topicName, long timeout) {
        long start = System.currentTimeMillis();
        long end = start + timeout;
        boolean allShardReady = false;
        while (start < end) {
            allShardReady = isAllShardSinked(topicName);
            if (allShardReady) {
                sleepInMs(2000);
                break;
            }

            sleepInMs(MAX_OPERATOR_INTERVAL);
            start = System.currentTimeMillis();
        }
        if (!allShardReady) {
            throw new DatahubClientException("Wait for all shard sinked timeout");
        }
    }

    private boolean isAllShardSinked(String topicName) {
        Map<String, ConnectorOffset> shardOffsetMap = new HashMap<>();
        ListShardResult listShardResult = client.listShard(TEST_PROJECT_NAME, topicName);
        for (ShardEntry entry : listShardResult.getShards()) {
            String shardId = entry.getShardId();
            final GetCursorResult getCursorResult = client.getCursor(TEST_PROJECT_NAME, topicName, shardId, CursorType.LATEST);
            shardOffsetMap.put(shardId, new ConnectorOffset() {{
                setSequence(getCursorResult.getSequence());
                setTimestamp(getCursorResult.getTimestamp());
            }});
        }

        GetConnectorShardStatusResult getConnectorShardStatusResult = client.getConnectorShardStatus(TEST_PROJECT_NAME, topicName, connectorType);
        for (Map.Entry<String, ConnectorShardStatusEntry> entrySet : getConnectorShardStatusResult.getStatusEntryMap().entrySet()) {
            String shardId = entrySet.getKey();
            ConnectorShardStatusEntry entry = entrySet.getValue();
            ConnectorOffset writeOffset = shardOffsetMap.get(shardId);
            if (writeOffset != null) {
                if (sUseNewExecutor) {
                    if (entry.getCurrSequence() < writeOffset.getSequence() || entry.getCurrTimestamp() < writeOffset.getTimestamp()) {
                        return false;
                    }
                } else {
                    if (entry.getCurrSequence() < writeOffset.getSequence()) {
                        return false;
                    }
                }

            }
        }
        return true;
    }
}
