package com.aliyun.datahub.client.example;

import com.aliyun.datahub.client.metircs.ClientMetrics;

public class DatahubClientExample {
    public static void main(String[] args) {
        // whether enable client metrics
        ClientMetrics.startMetrics();

        ProjectTopicExample projectTopicExample = new ProjectTopicExample();
        projectTopicExample.runExample();

        PutGetRecordsExample putGetRecordsExample = new PutGetRecordsExample();
        putGetRecordsExample.runExample();

        SubscriptionExample subscriptionExample = new SubscriptionExample();
        subscriptionExample.runExample();

        ConnectorExample connectorExample = new ConnectorExample();
        connectorExample.runExample();


        projectTopicExample.deleteProjectAndTopic();
    }
}
