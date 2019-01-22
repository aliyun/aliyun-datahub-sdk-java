package com.aliyun.datahub.wrapper;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.model.GetProjectResult;
import com.aliyun.datahub.common.data.RecordType;

import java.util.List;

public class Project {

    public static class Builder {
        public static Project build(String projectName, DatahubClient client) {
            return new Project(client.getProject(projectName), client);
        }

        public static Project build(String projectName, DatahubConfiguration conf) {
            DatahubClient client = new DatahubClient(conf);
            return build(projectName, client);
        }
    }

    private GetProjectResult data;
    private DatahubClient client;

    /**
     * constructor
     *
     * @param data   project data
     * @param client datahub client
     */
    private Project(GetProjectResult data, DatahubClient client) {
        this.data = data;
        this.client = client;
    }

    // topic operations

    /**
     * create topic
     *
     * @param topicName    topic name
     * @param shardCount   shard count
     * @param lifeCycle    data lifecycle
     * @param recordType   record type
     * @param recordSchema record schema, when recordType is TOUPLE this parameter will be used
     * @param desc         description
     * @return topic wrapper
     */
    public Topic createTopic(String topicName, int shardCount, int lifeCycle, RecordType recordType, RecordSchema recordSchema, String desc) {
        client.createTopic(data.getProjectName(), topicName, shardCount, lifeCycle, recordType, recordSchema, desc);
        return Topic.Builder.build(data.getProjectName(), topicName, client);
    }

    /**
     * list topic
     *
     * @return topic names
     */
    public List<String> listTopic() {
        return client.listTopic(data.getProjectName()).getTopics();
    }

    /**
     * get topic
     *
     * @param topicName topic name
     * @return topic wrapper
     */
    public Topic getTopic(String topicName) {
        return Topic.Builder.build(data.getProjectName(), topicName, client);
    }

    /**
     * update topic meta
     *
     * @param topicName topic name
     * @param lifecycle data lifecycle
     * @param desc      topic description
     */
    public void updateTopic(String topicName, int lifecycle, String desc) {
        client.updateTopic(data.getProjectName(), topicName, lifecycle, desc);
    }

    /**
     * delete topic
     *
     * @param topicName topic name
     */
    public void deleteTopic(String topicName) {
        client.deleteTopic(data.getProjectName(), topicName);
    }

    // getters
    public String getComment() {
        return data.getComment();
    }

    public String getProjectName() {
        return data.getProjectName();
    }

    public long getCreateTime() {
        return data.getCreateTime();
    }

    public long getLastModifyTime() {
        return data.getLastModifyTime();
    }
}
