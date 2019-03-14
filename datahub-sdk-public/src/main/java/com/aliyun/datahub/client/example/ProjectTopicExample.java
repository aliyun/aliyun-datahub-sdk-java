package com.aliyun.datahub.client.example;

import com.aliyun.datahub.client.model.*;

public class ProjectTopicExample extends BaseExample {
    @Override
    public void runExample() {
        createProjectAndTopic();
        operateOnProject();
        operateOnTopic();
    }

    private void createProjectAndTopic() {
        // create project
        client.createProject(TEST_PROJECT, "test comment");

        // create blob topic
        client.createTopic(TEST_PROJECT, TEST_TOPIC_BLOB, 4, 1, RecordType.BLOB, "test comment");

        // create tuple topic
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("field1", FieldType.STRING));
        schema.addField(new Field("field2", FieldType.BIGINT));
        client.createTopic(TEST_PROJECT, TEST_TOPIC_TUPLE, 4, 1, RecordType.TUPLE, schema, "test comment");
    }

    private void operateOnProject() {
        // get project
        GetProjectResult getProjectResult = client.getProject(TEST_PROJECT);

        // list project
        ListProjectResult listProjectResult = client.listProject();

        // update project
        client.updateProject(TEST_PROJECT, "update comment");
    }

    private void operateOnTopic() {
        // wait for all shards become active
        client.waitForShardReady(TEST_PROJECT, TEST_TOPIC_TUPLE);

        // list all shards
        ListShardResult listShardResult = client.listShard(TEST_PROJECT, TEST_TOPIC_TUPLE);

        // get topic
        GetTopicResult getTopicResult = client.getTopic(TEST_PROJECT, TEST_TOPIC_TUPLE);

        // list topic
        ListTopicResult listTopicResult = client.listTopic(TEST_PROJECT);

        // update topic
        client.updateTopic(TEST_PROJECT, TEST_TOPIC_TUPLE, "update comment");
        getTopicResult = client.getTopic(TEST_PROJECT, TEST_TOPIC_TUPLE);

        // split shard
        SplitShardResult splitShardResult = client.splitShard(TEST_PROJECT, TEST_TOPIC_TUPLE, "1");

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            // do nothing
        }

        // merge shard
        MergeShardResult mergeShardResult = client.mergeShard(TEST_PROJECT, TEST_TOPIC_TUPLE, "4", "5");

        // get meter info
        GetMeterInfoResult getMeterInfoResult = client.getMeterInfo(TEST_PROJECT, TEST_TOPIC_TUPLE, "0");

        // append field
        client.appendField(TEST_PROJECT, TEST_TOPIC_TUPLE, new Field("field3", FieldType.STRING));
    }

    public void deleteProjectAndTopic() {
        // delete blob topic
        client.deleteTopic(TEST_PROJECT, TEST_TOPIC_BLOB);

        // delete tuple topic
        client.deleteTopic(TEST_PROJECT, TEST_TOPIC_TUPLE);

        // delete project
        client.deleteProject(TEST_PROJECT);
    }
}
