package com.aliyun.datahub.client.example;

import com.aliyun.datahub.client.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SubscriptionExample extends BaseExample {
    @Override
    public void runExample() {
        operateOnSubscription();
    }

    private void operateOnSubscription() {
        // create subscription
        CreateSubscriptionResult createSubscriptionResult = client.createSubscription(TEST_PROJECT, TEST_TOPIC_TUPLE, "test subscription");
        String subId = createSubscriptionResult.getSubId();
        System.out.println("create subscription success. subId: " + subId);

        // get subscription
        GetSubscriptionResult getSubscriptionResult = client.getSubscription(TEST_PROJECT, TEST_TOPIC_TUPLE, subId);

        // update subscription comment
        client.updateSubscription(TEST_PROJECT, TEST_TOPIC_TUPLE, subId, "update subscription comment");

        // update subscription state
        client.updateSubscriptionState(TEST_PROJECT, TEST_TOPIC_TUPLE, subId, SubscriptionState.OFFLINE);

        client.updateSubscriptionState(TEST_PROJECT, TEST_TOPIC_TUPLE, subId, SubscriptionState.ONLINE);

        // open subscription session
        List<String> shardIds = new ArrayList<String>() {{
            add("0");
        }};
        OpenSubscriptionSessionResult openSubscriptionSessionResult = client.openSubscriptionSession(TEST_PROJECT, TEST_TOPIC_TUPLE, subId, shardIds);
        final SubscriptionOffset subOffset = openSubscriptionSessionResult.getOffsets().get("0");

        // get subscription offset
        GetSubscriptionOffsetResult getSubscriptionOffsetResult = client.getSubscriptionOffset(TEST_PROJECT, TEST_TOPIC_TUPLE, subId, shardIds);

        // commit subscription offset
        Map<String, SubscriptionOffset> offsetMap = new HashMap<>();
        offsetMap.put("0", new SubscriptionOffset() {{
            setTimestamp(100);
            setSequence(1);
            setSessionId(subOffset.getSessionId());
            setVersionId(subOffset.getVersionId());
        }});
        client.commitSubscriptionOffset(TEST_PROJECT, TEST_TOPIC_TUPLE, subId, offsetMap);

        // reset subscription offset
        client.resetSubscriptionOffset(TEST_PROJECT, TEST_TOPIC_TUPLE, subId, offsetMap);

        // get subscription offset
        getSubscriptionOffsetResult = client.getSubscriptionOffset(TEST_PROJECT, TEST_TOPIC_TUPLE, subId, shardIds);

        // list subscription
        ListSubscriptionResult listSubscriptionResult = client.listSubscription(TEST_PROJECT, TEST_TOPIC_TUPLE, 1, 10);

        // delete subscription
        for (SubscriptionEntry entry : listSubscriptionResult.getSubscriptions()) {
            if (entry.getType() == SubscriptionType.USER) {
                client.deleteSubscription(TEST_PROJECT, TEST_TOPIC_TUPLE, entry.getSubId());
            }
        }
    }
}
