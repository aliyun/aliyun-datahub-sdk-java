package com.aliyun.datahub.model;

import java.util.ArrayList;
import java.util.List;

public class ListTopicResult extends Result {
    private com.aliyun.datahub.client.model.ListTopicResult proxyResult;

    public ListTopicResult(com.aliyun.datahub.client.model.ListTopicResult proxyResult) {
        this.proxyResult = proxyResult;
        setRequestId(proxyResult.getRequestId());
    }

    public ListTopicResult() {
        proxyResult = new com.aliyun.datahub.client.model.ListTopicResult();
    }

    public void setTopics(List<String> topics) {
        proxyResult.setTopicNames(topics);
    }

    public void addTopic(String topic) {
        List<String> topicNames = proxyResult.getTopicNames();
        if (topicNames == null) {
            topicNames = new ArrayList<>();
            proxyResult.setTopicNames(topicNames);
        }
        topicNames.add(topic);
    }

    public List<String> getTopics() {
        return proxyResult.getTopicNames();
    }
}
