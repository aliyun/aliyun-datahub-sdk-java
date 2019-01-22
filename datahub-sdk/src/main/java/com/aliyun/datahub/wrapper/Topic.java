package com.aliyun.datahub.wrapper;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.model.*;

import java.util.List;

public class Topic {

    public static class Builder {
        public static Topic build(String projectName, String topicName, DatahubClient client) {
            return new Topic(client.getTopic(projectName, topicName), client);
        }

        public static Topic build(String projectName, String topicName, DatahubConfiguration conf) {
            DatahubClient client = new DatahubClient(conf);
            return build(projectName, topicName, client);
        }
    }

    private GetTopicResult data;
    private DatahubClient client;

    /**
     * constructor
     *
     * @param data   topic data
     * @param client datahub client
     */
    private Topic(GetTopicResult data, DatahubClient client) {
        this.data = data;
        this.client = client;
    }

    // shard operations

    /**
     * list shard
     *
     * @return shard status list
     */
    public List<ShardEntry> listShard() {
        return client.listShard(data.getProjectName(), data.getTopicName()).getShards();
    }

    /**
     * split shard
     *
     * @return new child shard desc list
     */
    public List<ShardDesc> splitShard(String shardId, String splitKey) {
        return client.splitShard(data.getProjectName(), data.getTopicName(), shardId, splitKey).getShards();
    }

    /**
     * trivial split shard
     *
     * @return new child shard desc list
     */
    public List<ShardDesc> splitShard(String shardId) {
        return client.splitShard(data.getProjectName(), data.getTopicName(), shardId).getShards();
    }

    /**
     * merge shard
     *
     * @return new child shard desc
     */
    public ShardDesc mergeShard(String shardId, String adjacentShardId) {
        return client.mergeShard(data.getProjectName(), data.getTopicName(), shardId, adjacentShardId).getChildShard();
    }

    /**
     * wait until all shards loaded within 30 seconds
     */
    public void waitForShardReady() {
        client.waitForShardReady(data.getProjectName(), data.getTopicName());
    }

    /**
     * wait untuil all shards loaded, within specified timeout
     * @param timeout timeout value in milliseconds
     */
    public void waitForShardReady(int timeout) {
        client.waitForShardReady(data.getProjectName(), data.getTopicName(), timeout);
    }

    // cursor operations

    /**
     * get cursor by timestamp
     *
     * @param shardId   shard id
     * @param timestamp timestamp milliseconds from 1970-01-01 00:00:00
     * @return cursor
     */
    public String getCursor(String shardId, long timestamp) {
        return client.getCursor(data.getProjectName(), data.getTopicName(), shardId, timestamp).getCursor();
    }

    /**
     * get cursor by cursor type
     *
     * @param shardId shard id
     * @param type
     * @return cursor
     * @see com.aliyun.datahub.model.GetCursorRequest.CursorType
     */
    public String getCursor(String shardId, GetCursorRequest.CursorType type) {
        return client.getCursor(data.getProjectName(), data.getTopicName(), shardId, type).getCursor();
    }

    // record operations

    /**
     * get records
     *
     * @param shardId shard id
     * @param cursor  cursor
     * @param size    record size
     * @return records
     */
    public GetRecordsResult getRecords(String shardId, String cursor, int size) {
        return client.getRecords(data.getProjectName(), data.getTopicName(), shardId, cursor, size, data.getRecordSchema());
    }

    /**
     * put records
     *
     * @param entries records
     * @return put record result
     */
    public PutRecordsResult putRecords(List<RecordEntry> entries) {
        return client.putRecords(data.getProjectName(), data.getTopicName(), entries);
    }

    /**
     * put records with retry
     *
     * @param entries records
     * @param retries retry count
     * @return put record result
     */
    public PutRecordsResult putRecords(List<RecordEntry> entries, int retries) {
        return client.putRecords(data.getProjectName(), data.getTopicName(), entries, retries);
    }

    /**
     * get blob records
     *
     * @param shardId shard id
     * @param cursor  cursor
     * @param size    record size
     * @return records
     */
    public GetBlobRecordsResult getBlobRecords(String shardId, String cursor, int size) {
        return client.getBlobRecords(data.getProjectName(), data.getTopicName(), shardId, cursor, size);
    }

    /**
     * put blob records
     *
     * @param entries records
     * @return put record result
     */
    public PutBlobRecordsResult putBlobRecords(List<BlobRecordEntry> entries) {
        return client.putBlobRecords(data.getProjectName(), data.getTopicName(), entries);
    }

    /**
     * put blob records with retry
     *
     * @param entries records
     * @param retries retry count
     * @return put record result
     */
    public PutBlobRecordsResult putBlobRecords(List<BlobRecordEntry> entries, int retries) {
        return client.putBlobRecords(data.getProjectName(), data.getTopicName(), entries, retries);
    }

    // getters
    public String getProjectName() {
        return data.getProjectName();
    }

    public String getTopicName() {
        return data.getTopicName();
    }

    public int getShardCount() {
        return data.getShardCount();
    }

    public int getLifeCycle() {
        return data.getLifeCycle();
    }

    public RecordType getRecordType() {
        return data.getRecordType();
    }

    public RecordSchema getRecordSchema() {
        return data.getRecordSchema();
    }

    public String getComment() {
        return data.getComment();
    }

    public long getCreateTime() {
        return data.getCreateTime();
    }

    public long getLastModifyTime() {
        return data.getLastModifyTime();
    }
}
