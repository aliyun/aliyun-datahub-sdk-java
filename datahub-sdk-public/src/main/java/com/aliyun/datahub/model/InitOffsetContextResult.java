package com.aliyun.datahub.model;

import com.aliyun.datahub.client.model.OpenSubscriptionSessionResult;
import com.aliyun.datahub.client.model.SubscriptionOffset;
import com.aliyun.datahub.utils.ModelConvertToNew;
import com.aliyun.datahub.utils.ModelConvertToOld;

import java.util.HashMap;
import java.util.Map;

public class InitOffsetContextResult extends Result {
    private OpenSubscriptionSessionResult proxyResult;
    private String projectName;
    private String topicName;
    private String subId;

    public InitOffsetContextResult(String project, String topic, String subId, OpenSubscriptionSessionResult proxyResult) {
        this.projectName = project;
        this.topicName = topic;
        this.subId = subId;
        this.proxyResult = proxyResult;
        setRequestId(proxyResult.getRequestId());
    }

    public InitOffsetContextResult() {
        proxyResult = new OpenSubscriptionSessionResult();
    }

    public Map<String, OffsetContext> getOffsets() {
        Map<String, OffsetContext> offsetContextMap = new HashMap<>();
        if (proxyResult.getOffsets() != null) {
            for (Map.Entry<String, com.aliyun.datahub.client.model.SubscriptionOffset> entry : proxyResult.getOffsets().entrySet()) {
                OffsetContext offsetContext = ModelConvertToOld.convertOffsetContext(projectName, topicName, subId, entry.getKey(), entry.getValue());
                offsetContextMap.put(entry.getKey(), offsetContext);
            }
        }

        return offsetContextMap;
    }

    public void setOffsets(Map<String, OffsetContext> offsets) {

        if (offsets != null) {
            Map<String, com.aliyun.datahub.client.model.SubscriptionOffset> subscriptionOffsetMap = new HashMap<>();

            for (Map.Entry<String, OffsetContext> entry : offsets.entrySet()) {
                SubscriptionOffset subOffset = ModelConvertToNew.convertOffsetContext(entry.getValue());
                subscriptionOffsetMap.put(entry.getKey(), subOffset);
            }

            proxyResult.setOffsets(subscriptionOffsetMap);
        }
    }
}
