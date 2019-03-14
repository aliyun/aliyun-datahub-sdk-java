package com.aliyun.datahub.client.e2e;

import com.aliyun.datahub.client.exception.SubscriptionOfflineException;
import com.aliyun.datahub.client.exception.SubscriptionOffsetResetException;
import com.aliyun.datahub.client.exception.SubscriptionSessionInvalidException;
import com.aliyun.datahub.client.model.*;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.testng.Assert.fail;

public class SubscriptionTest extends BaseTest {

    @BeforeClass
    public static void setUpBeforeClass() {
        bCreateTopic = true;
        BaseTest.setUpBeforeClass();
    }


    @AfterMethod
    @Override
    public void tearDownAfterMethod() {
        try {
            ListSubscriptionResult listSubscriptionResult = client.listSubscription(TEST_PROJECT_NAME, blobTopicName, 1, 10);
            for (SubscriptionEntry entry : listSubscriptionResult.getSubscriptions()) {
                client.deleteSubscription(TEST_PROJECT_NAME, blobTopicName, entry.getSubId());
            }
        } catch (Exception e) {
            //
        }
        super.tearDownAfterMethod();
    }

    @Test
    public void testNormal() {
        // create subscription
        CreateSubscriptionResult createSubscriptionResult = client.createSubscription(TEST_PROJECT_NAME, blobTopicName, "test commit");
        String subId = createSubscriptionResult.getSubId();

        // get subscription
        GetSubscriptionResult getSubscriptionResult = client.getSubscription(TEST_PROJECT_NAME, blobTopicName, subId);
        Assert.assertEquals(getSubscriptionResult.getComment(), "test commit");
        Assert.assertEquals(getSubscriptionResult.getSubId(), subId);
        Assert.assertEquals(getSubscriptionResult.getType(), SubscriptionType.USER);
        Assert.assertEquals(getSubscriptionResult.getState(), SubscriptionState.ONLINE);

        // update subscription
        client.updateSubscription(TEST_PROJECT_NAME, blobTopicName, subId, "update commit");
        getSubscriptionResult = client.getSubscription(TEST_PROJECT_NAME, blobTopicName, subId);
        Assert.assertEquals(getSubscriptionResult.getComment(), "update commit");

        // open offset session
        OpenSubscriptionSessionResult openSubscriptionSessionResult = client.openSubscriptionSession(TEST_PROJECT_NAME, blobTopicName, subId, Collections.singletonList("0"));
        SubscriptionOffset subscriptionOffset = openSubscriptionSessionResult.getOffsets().get("0");
        String sessionId = subscriptionOffset.getSessionId();
        long versionId = subscriptionOffset.getVersionId();
        Assert.assertEquals(sessionId, "1");
        Assert.assertEquals(versionId, 0);

        final SubscriptionOffset offset = new SubscriptionOffset();
        offset.setSequence(10);
        offset.setTimestamp(1000);
        offset.setSessionId(sessionId);
        offset.setVersionId(versionId);

        // commit offset
        client.commitSubscriptionOffset(TEST_PROJECT_NAME, blobTopicName, subId, new HashMap<String, SubscriptionOffset>() {{
            put("0", offset);
        }});

        GetSubscriptionOffsetResult getSubscriptionOffsetResult = client.getSubscriptionOffset(TEST_PROJECT_NAME, blobTopicName, subId, Collections.singletonList("0"));
        Assert.assertEquals(getSubscriptionOffsetResult.getOffsets().get("0").getSequence(), 10);
        Assert.assertEquals(getSubscriptionOffsetResult.getOffsets().get("0").getTimestamp(), 1000);

        // reset offset
        client.resetSubscriptionOffset(TEST_PROJECT_NAME, blobTopicName, subId, new HashMap<String, SubscriptionOffset>() {{
            put("0", new SubscriptionOffset() {{
                setTimestamp(2000);
                setSequence(20);
            }});
        }});

        getSubscriptionOffsetResult = client.getSubscriptionOffset(TEST_PROJECT_NAME, blobTopicName, subId, Collections.singletonList("0"));
        Assert.assertEquals(getSubscriptionOffsetResult.getOffsets().get("0").getSequence(), 20);
        Assert.assertEquals(getSubscriptionOffsetResult.getOffsets().get("0").getTimestamp(), 2000);
        Assert.assertEquals(getSubscriptionOffsetResult.getOffsets().get("0").getVersionId(), 1);
        versionId = getSubscriptionOffsetResult.getOffsets().get("0").getVersionId();

        // commit offset with new version
        offset.setSequence(20);
        offset.setTimestamp(2000);
        offset.setVersionId(versionId);
        offset.setSessionId(sessionId);

        client.commitSubscriptionOffset(TEST_PROJECT_NAME, blobTopicName, subId, new HashMap<String, SubscriptionOffset>() {{
            put("0", offset);
        }});

        // set state offline
        client.updateSubscriptionState(TEST_PROJECT_NAME, blobTopicName, subId, SubscriptionState.OFFLINE);
        getSubscriptionResult = client.getSubscription(TEST_PROJECT_NAME, blobTopicName, subId);
        Assert.assertEquals(getSubscriptionResult.getState(), SubscriptionState.OFFLINE);
    }

    @Test
    public void testCommitWithoutOpenSession() {
        // create subscription
        CreateSubscriptionResult createSubscriptionResult = client.createSubscription(TEST_PROJECT_NAME, blobTopicName, "test commit");
        String subId = createSubscriptionResult.getSubId();
        final SubscriptionOffset offset = new SubscriptionOffset();
        offset.setSequence(10);
        offset.setTimestamp(1000);
        offset.setSessionId("1");
        offset.setVersionId(0);

        // commit offset
        try {
            client.commitSubscriptionOffset(TEST_PROJECT_NAME, blobTopicName, subId, new HashMap<String, SubscriptionOffset>() {{
                put("0", offset);
            }});
            fail("Commit without open session should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SubscriptionSessionInvalidException);
        }
    }

    @Test
    public void testCommitAfterReset() {
        // create subscription
        CreateSubscriptionResult createSubscriptionResult = client.createSubscription(TEST_PROJECT_NAME, blobTopicName, "test commit");
        String subId = createSubscriptionResult.getSubId();

        final OpenSubscriptionSessionResult openSubscriptionSessionResult = client.openSubscriptionSession(TEST_PROJECT_NAME, blobTopicName, subId, Collections.singletonList("0"));
        // reset offset
        client.resetSubscriptionOffset(TEST_PROJECT_NAME, blobTopicName, subId, new HashMap<String, SubscriptionOffset>() {{
            put("0", new SubscriptionOffset() {{
                setTimestamp(2000);
                setSequence(20);
                setSessionId(openSubscriptionSessionResult.getOffsets().get("0").getSessionId());
                setVersionId(openSubscriptionSessionResult.getOffsets().get("0").getVersionId());
            }});
        }});

        // commit offset
        try {
            client.commitSubscriptionOffset(TEST_PROJECT_NAME, blobTopicName, subId, new HashMap<String, SubscriptionOffset>() {{
                put("0", new SubscriptionOffset() {{
                    setSessionId(openSubscriptionSessionResult.getOffsets().get("0").getSessionId());
                    setVersionId(openSubscriptionSessionResult.getOffsets().get("0").getVersionId());
                    setTimestamp(1000);
                    setSequence(10);
                }});
            }});
            fail("Commit with offset reset should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SubscriptionOffsetResetException);
        }
    }

    @Test
    public void testCommitAfterClose() {
        // create subscription
        CreateSubscriptionResult createSubscriptionResult = client.createSubscription(TEST_PROJECT_NAME, blobTopicName, "test commit");
        String subId = createSubscriptionResult.getSubId();
        final OpenSubscriptionSessionResult openSubscriptionSessionResult = client.openSubscriptionSession(TEST_PROJECT_NAME, blobTopicName, subId, Collections.singletonList("0"));

        client.updateSubscriptionState(TEST_PROJECT_NAME, blobTopicName, subId, SubscriptionState.OFFLINE);

        GetSubscriptionResult getSubscriptionResult = client.getSubscription(TEST_PROJECT_NAME, blobTopicName, subId);
        // commit offset
        try {
            client.commitSubscriptionOffset(TEST_PROJECT_NAME, blobTopicName, subId, new HashMap<String, SubscriptionOffset>() {{
                put("0", new SubscriptionOffset() {{
                    setSessionId(openSubscriptionSessionResult.getOffsets().get("0").getSessionId());
                    setVersionId(openSubscriptionSessionResult.getOffsets().get("0").getVersionId());
                    setTimestamp(1000);
                    setSequence(10);
                }});
            }});
            fail("Commit with subId offline should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SubscriptionOfflineException);
        }
    }

    @Test
    public void testCommitAfterReopen() {
        // create subscription
        CreateSubscriptionResult createSubscriptionResult = client.createSubscription(TEST_PROJECT_NAME, blobTopicName, "test commit");
        String subId = createSubscriptionResult.getSubId();
        final OpenSubscriptionSessionResult openSubscriptionSessionResult = client.openSubscriptionSession(TEST_PROJECT_NAME, blobTopicName, subId, Collections.singletonList("0"));

        client.openSubscriptionSession(TEST_PROJECT_NAME, blobTopicName, subId, Collections.singletonList("0"));

        // commit offset
        try {
            client.commitSubscriptionOffset(TEST_PROJECT_NAME, blobTopicName, subId, new HashMap<String, SubscriptionOffset>() {{
                put("0", new SubscriptionOffset() {{
                    setSessionId(openSubscriptionSessionResult.getOffsets().get("0").getSessionId());
                    setVersionId(openSubscriptionSessionResult.getOffsets().get("0").getVersionId());
                    setTimestamp(1000);
                    setSequence(10);
                }});
            }});
            fail("Commit with reopen session should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SubscriptionSessionInvalidException);
        }
    }
}
