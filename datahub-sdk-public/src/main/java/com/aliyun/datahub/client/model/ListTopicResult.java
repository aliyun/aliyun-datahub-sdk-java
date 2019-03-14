package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ListTopicResult extends BaseResult {
    @JsonProperty("TopicNames")
    private List<String> topicNames;

    public List<String> getTopicNames() {
        return topicNames;
    }

    public void setTopicNames(List<String> topicNames) {
        this.topicNames = topicNames;
    }
}
