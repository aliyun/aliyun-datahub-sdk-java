package com.aliyun.datahub.model;

import java.util.ArrayList;
import java.util.List;

public class ListTopicResult {
    private List<String> topics;

    public ListTopicResult() {
        this.topics = new ArrayList<String>();
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public void addTopic(String topic) {
        this.topics.add(topic);
    }

    public List<String> getTopics() {
        return this.topics;
    }
}
