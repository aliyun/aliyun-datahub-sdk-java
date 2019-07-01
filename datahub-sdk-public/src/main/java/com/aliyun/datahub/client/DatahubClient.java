package com.aliyun.datahub.client;

import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.model.*;

import java.util.List;
import java.util.Map;

/**
 * DatahubClient provides restful apis for visiting datahub service.
 * User should catch base DatahubClientException to get all useful information.
 * Example:
 * <p>
 * DatahubClient client = new DatahubClientBuilder().setDatahubConfig(xxx).build();
 * try {
 *     client.createProject("projectName", "comment");
 * } catch (DatahubClientException e) {
 *     System.out.println(e.toString());
 *     // deal with DatahubClientException
 * } catch (Exception e) {
 *     // deal with Exception
 * }
 */
public interface DatahubClient {
    /**
     * Get the information of the specified project.
     *
     * @param projectName The name of the project.
     * @return {@link GetProjectResult} The information of the project.
     * @throws DatahubClientException Throws DatahubClientException
     */
    GetProjectResult getProject(String projectName);

    /**
     * List all projects the user owns.
     * @return {@link ListProjectResult} The name list of the project.
     * @throws DatahubClientException Throws DatahubClientException
     */
    ListProjectResult listProject();

    /**
     * Create a datahub project.
     *
     * @param projectName The name of the project.
     * @param comment  The comment of the project.
     * @throws DatahubClientException Throws DatahubClientException
     */
    CreateProjectResult createProject(String projectName, String comment);

    /**
     * Update project information. Only support @comment
     * @param projectName The name of the project.
     * @param comment The comment to modify.
     * @throws DatahubClientException Throws DatahubClientException
     */
    UpdateProjectResult updateProject(String projectName, String comment);

    /**
     * Delete the specified project. If any topics exist in the project, the delete operation will fail.
     *
     * @param projectName The name of the project.
     * @throws DatahubClientException Throws DatahubClientException
     */
    DeleteProjectResult deleteProject(String projectName);

    /**
     * Wait for all shards' status of this topic is ACTIVE.
     * <P>
     * Default timeout is 30s.
     *
     * @param projectName The name of the project for shards to wait.
     * @param topicName The name of the topic for shards to wait.
     */
    void waitForShardReady(String projectName, String topicName);


    /**
     * Wait for all shards' status of this topic is ACTIVE.
     *
     * @param projectName The name of the project for shards to wait.
     * @param topicName The name of the topic for shards to wait.
     * @param timeout Max time to wait (Milliseconds).
     * @throws DatahubClientException Throws DatahubClientException
     */
    void waitForShardReady(String projectName, String topicName, long timeout);

    /**
     * Create a datahub topic with type: BLOB
     *
     * @param projectName The name of the project in which you create.
     * @param topicName The name of the topic
     * @param shardCount The initial shard count of the topic
     * @param lifeCycle The expire time of the data (Unit: DAY). The data written before that time is not accessible.
     * @param recordType The type of the record you want to write. Now support TUPLE and BLOB {@link RecordType}.
     * @param comment The comment of the topic.
     * @throws DatahubClientException Throws DatahubClientException
     */
    CreateTopicResult createTopic(String projectName, String topicName, int shardCount, int lifeCycle, RecordType recordType, String comment);

    /**
     * Create a datahub topic with type: TUPLE
     *
     * @param projectName The name of the project in which you create.
     * @param topicName The name of the topic
     * @param shardCount The initial shard count of the topic
     * @param lifeCycle The expire time of the data (Unit: DAY). The data written before that time is not accessible.
     * @param recordType The type of the record you want to write. Now support TUPLE and BLOB {@link RecordType}.
     * @param recordSchema The records schema of this topic. {@link RecordSchema}
     * @param comment The comment of the topic.
     * @throws DatahubClientException Throws DatahubClientException
     */
    CreateTopicResult createTopic(String projectName, String topicName, int shardCount, int lifeCycle, RecordType recordType, RecordSchema recordSchema, String comment);

    /**
     * Update topic meta information. Now only support modify @comment info.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param comment The comment to modify.
     * @throws DatahubClientException Throws DatahubClientException
     */
    UpdateTopicResult updateTopic(String projectName, String topicName, String comment);

    /**
     * Get the information of the specified topic.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @return {@link GetTopicResult}
     * @throws DatahubClientException Throws DatahubClientException
     */
    GetTopicResult getTopic(String projectName, String topicName);

    /**
     * Delete a specified topic.
     *
     * @param projectName The name of the project which the topic belongs to.
     * @param topicName The name of the topic.
     * @throws DatahubClientException Throws DatahubClientException
     */
    DeleteTopicResult deleteTopic(String projectName, String topicName);

    /**
     * List all topics in the project.
     *
     * @param projectName The name of the project.
     * @return {@link ListTopicResult} The name list of the topic.
     * @throws DatahubClientException Throws DatahubClientException
     */
    ListTopicResult listTopic(String projectName);

    /**
     * List shard information {@link ShardEntry} of a topic.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @return {@link ListShardResult} The list of the shard entries.
     * @throws DatahubClientException Throws DatahubClientException
     */
    ListShardResult listShard(String projectName, String topicName);

    /**
     * Split a shard. In function, sdk will automatically compute the split key which is used to split shard.
     * <p>
     * User can also call {@link #splitShard(String, String, String, String)} to specify the split key <br>
     * But this may cause unbalanced split if you specify an inaccurate key.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param shardId The shard which to split.
     * @return {@link SplitShardResult} New shard info.
     */
    SplitShardResult splitShard(String projectName, String topicName, String shardId);

    /**
     * Split a shard by the specified splitKey.
     * <p>
     * Warning: This method may cause unbalanced split if you specify an inaccurate key. Use {@link #splitShard(String, String, String, String)} instead.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param shardId The shard which to split.
     * @param splitKey The split key which is used to split shard.
     * @return {@link SplitShardResult} New shard info.
     * @throws DatahubClientException Throws DatahubClientException
     */
    SplitShardResult splitShard(String projectName, String topicName, String shardId, String splitKey);

    /**
     * Merge the specified shard and its adjacent shard. Only adjacent shards can be merged.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param shardId The shard which will be merged
     * @param adjacentShardId The adjacent shard of the specified shard.
     * @return {@link MergeShardResult} New shard info.
     * @throws DatahubClientException Throws DatahubClientException
     */
    MergeShardResult mergeShard(String projectName, String topicName, String shardId, String adjacentShardId);

    /**
     * Get the data cursor of a shard. Only can call this function with OLDEST and LATEST type.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param shardId The id of the shard.
     * @param type Which type used to get cursor. Also see {@link CursorType}
     * @return {@link GetCursorResult} Cursor info which contains the cursor which is used to read data.
     * @throws DatahubClientException Throws DatahubClientException
     */
    GetCursorResult getCursor(String projectName, String topicName, String shardId, CursorType type);

    /**
     * Get the data cursor of a shard. Only call this function with SYSTEM_TIME and SEQUENCE type.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param shardId The id of the shard.
     * @param type Which type used to get cursor. Also see {@link CursorType}
     * @param param Parameter used to get cursor.
     * @return {@link GetCursorResult} Cursor info which contains the cursor which is used to read data.
     * @throws DatahubClientException Throws DatahubClientException
     */
    GetCursorResult getCursor(String projectName, String topicName, String shardId, CursorType type, long param);

    /**
     * Write data records into a DataHub topic.
     * <p>
     * The response includes unsuccessfully processed records. Datahub attempts to process
     * all records in each record. A single record failure does not
     * stop the processing of subsequent records.
     * <br>
     * You can get the failed records which are not written. {@link PutErrorEntry} include
     * <code>ErrorCode</code> and <code>ErrorMessage</code> for each failed record.
     * <p>
     * We recommended you to use <code>putRecordsByShard</code> method to write data.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param records Records list to written. Each RecordEntity contains {@link RecordData} field, which can indicate BLOB data or TUPLE data.
     * @return {@link PutRecordsResult} The information of the failed records.
     * @throws DatahubClientException Throws DatahubClientException
     */
    PutRecordsResult putRecords(String projectName, String topicName, List<RecordEntry> records);


    /**
     * Write data records into a DataHub shard.
     * You can also send data by shard with the struct {@link RecordEntry}, but the ShardId, HashKey, PartitionKey
     * of the struct are useless.
     * <p>
     * This method is recommended to use to write data.
     *
     * @param projectName The name of the project
     * @param topicName
     * @param shardId
     * @param records
     */
    PutRecordsByShardResult putRecordsByShard(String projectName, String topicName, String shardId, List<RecordEntry> records);

    /**
     * Get the BLOB records of a shard.
     * <p>
     * The cursor parameter used to specify the start position to read.
     * If there is no records bound with the cursor, <code>GetDataRecords</code> returns an empty list.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param shardId The id of the shard.
     * @param cursor The start cursor used to read data.
     * @param limit Max record size to read.
     * @return {@link GetRecordsResult} Records information read.
     * @throws DatahubClientException Throws DatahubClientException
     */
    GetRecordsResult getRecords(String projectName, String topicName, String shardId, String cursor, int limit);

    /**
     * Get the TUPLE records of a shard.
     * <p>
     * The cursor parameter used to specify the start position to read.
     * If there is no records bound with the cursor, <code>GetDataRecords</code> returns an empty list.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param shardId The id of the shard.
     * @param schema If you read TUPLE records, you should pass <code>RecordSchema</code> as a parameter.
     * @param cursor The start cursor used to read data.
     * @param limit Max record size to read.
     * @return {@link GetRecordsResult} Records information read.
     * @throws DatahubClientException Throws DatahubClientException
     */
    GetRecordsResult getRecords(String projectName, String topicName, String shardId, RecordSchema schema, String cursor, int limit);

    /**
     * Append a field to a TUPLE topic.
     * <br>
     * Field should allow null. {@link Field}
     *
     * @param projectName The name of project.
     * @param topicName The name of topic.
     * @param field The field to append. Field value must allow null.
     * @throws DatahubClientException Throws DatahubClientException
     */
    AppendFieldResult appendField(String projectName, String topicName, Field field);

    /**
     * Get metering info of the specified shard
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param shardId The id of the shard.
     * @return {@link GetMeterInfoResult} Metering info which contains <code>ActiveTime</code> and <code>Storage</code>
     * @throws DatahubClientException Throws DatahubClientException
     */
    GetMeterInfoResult getMeterInfo(String projectName, String topicName, String shardId);


    /**
     * Create datahub data connectors.
     * <p>
     * Now datahub supports multiple synchronization types, such as ODPS(MaxCompute), OSS, OTS, ADS, MySQL, ElasticSearch, FunctionCompute and etc.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param connectorType The type of connector which you want create.
     * @param columnFields Which fields you want synchronize.
     * @param config Detail config of specified connector type. {@link SinkConfig}
     * @throws DatahubClientException Throws DatahubClientException
     */
    CreateConnectorResult createConnector(String projectName, String topicName,
                         ConnectorType connectorType, List<String> columnFields, SinkConfig config);

    /**
     * Create datahub data connectors.
     * <p>
     * Now datahub supports multiple synchronization types, such as ODPS(MaxCompute), OSS, OTS, ADS, MySQL, ElasticSearch, FunctionCompute and etc.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param connectorType The type of connector which you want create.
     * @param columnFields Which fields you want synchronize.
     * @param sinkStartTime Start time to sink from datahub. Unit: Ms
     * @param config Detail config of specified connector type. {@link SinkConfig}
     * @throws DatahubClientException Throws DatahubClientException
     */
    CreateConnectorResult createConnector(String projectName, String topicName,
                                          ConnectorType connectorType, long sinkStartTime, List<String> columnFields, SinkConfig config);

    /**
     * Get information of the specified data connector.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param connectorType The type of the connector.
     * @return {@link GetConnectorResult} The detail information of a connector.
     * @throws DatahubClientException Throws DatahubClientException
     */
    GetConnectorResult getConnector(String projectName, String topicName, ConnectorType connectorType);

    /**
     * Update connector config of the specified data connector
     * @param projectName The name of the project
     * @param topicName The name of the topic
     * @param connectorType The type of connector which you want update.
     * @param config Detail config of specified connector type. {@link SinkConfig}
     * @throws DatahubClientException Throws DatahubClientException
     */
    UpdateConnectorResult updateConnector(String projectName, String topicName, ConnectorType connectorType, SinkConfig config);

    /**
     * List name of connectors
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @return {@link ListConnectorResult} The name list of connecors.
     * @throws DatahubClientException Throws DatahubClientException
     */
    ListConnectorResult listConnector(String projectName, String topicName);

    /**
     * Delete a data connector.
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param connectorType The type of the connector.
     * @throws DatahubClientException Throws DatahubClientException
     */
    DeleteConnectorResult deleteConnector(String projectName, String topicName, ConnectorType connectorType);

    /**
     * Get the done time of a data connector. This method mainly used to get MaxCompute synchronize point.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param connectorType The type of the connector.
     * @return {@link GetConnectorDoneTimeResult} Contains a timestamp before which all data is totally synchronized.
     * @throws DatahubClientException Throws DatahubClientException
     */
    GetConnectorDoneTimeResult getConnectorDoneTime(String projectName, String topicName, ConnectorType connectorType);

    /**
     * Reload a data connector.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param connectorType The type of the connector.
     * @throws DatahubClientException Throws DatahubClientException
     */
    ReloadConnectorResult reloadConnector(String projectName, String topicName, ConnectorType connectorType);

    /**
     * Reload the specified shard of the data connector.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param connectorType The type of the connector.
     * @param shardId The id of the shard.
     * @throws DatahubClientException Throws DatahubClientException
     */
    ReloadConnectorResult reloadConnector(String projectName, String topicName, ConnectorType connectorType, String shardId);

    /**
     * Update the state of the data connector
     *
     * @param projectName The name of the project
     * @param topicName The name of the topic
     * @param connectorType The type of the connector
     * @param connectorState The state of the connector. Support: ConnectorState.PAUSED, ConnectorState.RUNNING
     * @throws DatahubClientException Throws DatahubClientException
     */
    UpdateConnectorStateResult updateConnectorState(String projectName, String topicName, ConnectorType connectorType, ConnectorState connectorState);

    /**
     * Update connector sink offset. The operation must be operated after connector stopped.
     *
     * @param projectName The name of the project
     * @param topicName The name of the topic
     * @param connectorType The type of the connector
     * @param shardId The id of the shard. If shardId is null, then update all shards offset
     * @param offset The connector offset
     * @throws DatahubClientException Throws DatahubClientException
     */
    UpdateConnectorOffsetResult updateConnectorOffset(String projectName, String topicName, ConnectorType connectorType, String shardId, ConnectorOffset offset);

    /**
     * Get the detail information of the shard task which belongs to the specified data connector.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param connectorType The type of the connector.
     * @return {@link GetConnectorShardStatusResult} The detail information of the shard task.
     * @throws DatahubClientException Throws DatahubClientException
     */
    GetConnectorShardStatusResult getConnectorShardStatus(String projectName, String topicName, ConnectorType connectorType);

    /**
     *
     * Get the detail information of the shard task which belongs to the specified data connector.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param connectorType The type of the connector.
     * @param shardId The id of the shard
     * @return {@link ConnectorShardStatusEntry} The detail information of the shard task.
     * @throws DatahubClientException Throws DatahubClientException
     */
    ConnectorShardStatusEntry getConnectorShardStatus(String projectName, String topicName, ConnectorType connectorType, String shardId);

    /**
     * Append data connector field
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param connectorType The type of the connector.
     * @param fieldName The name of the field.
     * @throws DatahubClientException Throws DatahubClientException
     */
    AppendConnectorFieldResult appendConnectorField(String projectName, String topicName, ConnectorType connectorType, String fieldName);

    /**
     * Create a subscription, and then you should commit offsets with this subscription.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param comment The comment of the subscription
     * @return {@link CreateSubscriptionResult} Contains a subId returned by server.
     * @throws DatahubClientException Throws DatahubClientException
     */
    CreateSubscriptionResult createSubscription(String projectName, String topicName, String comment);

    /**
     * Get the detail information of a subscription.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param subId The id of the subscription.
     * @return {@link GetSubscriptionResult} Detail information of a subscription.
     * @throws DatahubClientException Throws DatahubClientException
     */
    GetSubscriptionResult getSubscription(String projectName, String topicName, String subId);

    /**
     * Delete a subscription.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param subId The id of the subscription.
     * @throws DatahubClientException Throws DatahubClientException
     */
    DeleteSubscriptionResult deleteSubscription(String projectName, String topicName, String subId);

    /**
     * List subscriptions in the topic.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param pageNum The page number used to list subscriptions.
     * @param pageSize The page size used to list subscriptions.
     * @return {@link ListSubscriptionResult} Detail information of the subscriptions.
     * @throws DatahubClientException Throws DatahubClientException
     */
    ListSubscriptionResult listSubscription(String projectName, String topicName, int pageNum, int pageSize);

    /**
     * Update a subscription. Now only support update comment information.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param subId The id of the subscription.
     * @param comment The comment you want to update.
     * @throws DatahubClientException Throws DatahubClientException
     */
    UpdateSubscriptionResult updateSubscription(String projectName, String topicName, String subId, String comment);

    /**
     * Update a subscription' state. You can change the state of a subscription to ONLINE or OFFLINE. <br>
     * When offline, you can not commit offsets of the subscription.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param subId The id of the subscription.
     * @param state The state you want to change.
     * @throws DatahubClientException Throws DatahubClientException
     */
    UpdateSubscriptionStateResult updateSubscriptionState(String projectName, String topicName, String subId, SubscriptionState state);

    /**
     * Init a subscription session, and returns offset if any offset stored before.
     * <p>
     * Subscription should be initialized before use. This operation makes sure that only one client use this subscription.
     * <p>
     * This method will return sessionId in {@link SubscriptionOffset}
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param subId The id of the subscription.
     * @param shardIds The id list of the shards.
     * @return {@link OpenSubscriptionSessionResult} Offsets of the shards belongs to the topic.
     * @throws DatahubClientException Throws DatahubClientException
     */
    OpenSubscriptionSessionResult openSubscriptionSession(String projectName, String topicName, String subId, List<String> shardIds);

    /**
     * Get offsets of a subscription.
     * <p>
     * This method dost not return sessionId in {@link SubscriptionOffset}
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param subId The id of the subscription.
     * @param shardIds The id list of the shards.
     * @return {@link GetSubscriptionOffsetResult} Offsets of the shards belongs to the topic.
     * @throws DatahubClientException Throws DatahubClientException
     */
    GetSubscriptionOffsetResult getSubscriptionOffset(String projectName, String topicName, String subId, List<String> shardIds);

    /**
     * Update offsets of shards to server. This operation allows you store offsets on the server side.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param subId The id of the subscription.
     * @param offsets The offset map of shards.
     * @throws DatahubClientException Throws DatahubClientException
     */
    CommitSubscriptionOffsetResult commitSubscriptionOffset(String projectName, String topicName, String subId, Map<String, SubscriptionOffset> offsets);

    /**
     * Reset offsets of shards to server. This operation allows you reset offsets on the server side
     * @param projectName The name of the project
     * @param topicName The name of the topic
     * @param subId The id of the subscription
     * @param offsets The offset map of shards
     * @throws DatahubClientException Throws DatahubClientException
     */
    ResetSubscriptionOffsetResult resetSubscriptionOffset(String projectName, String topicName, String subId, Map<String, SubscriptionOffset> offsets);

    /**
     * Heartbeat request to let server know consumer status.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param consumerGroup The consumerGroup use subId.
     * @param consumerId The consumerId.
     * @param versionId The offset version id.
     * @param holdShardList The shard list held by consumer.
     * @param readEndShardList The shard list finished.
     * @return {@link HeartbeatResult} Heartbeat result contains versionId, shardList, totalPlan
     * @throws DatahubClientException Throws DatahubClientException
     */
    HeartbeatResult heartbeat(String projectName, String topicName, String consumerGroup, String consumerId, long versionId, List<String> holdShardList, List<String> readEndShardList);

    /**
     * Join a consumer group.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param consumerGroup The consumerGroup use subId.
     * @param sessionTimeout The session timeout.
     * @return {@link JoinGroupResult} Consumer id.
     * @throws DatahubClientException Throws DatahubClientException
     */
    JoinGroupResult joinGroup(String projectName, String topicName, String consumerGroup, long sessionTimeout);

    /**
     * Sync consumer group info.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param consumerGroup The consumerGroup use subId.
     * @param consumerId The consumerId.
     * @param versionId The offset version id.
     * @param releaseShardList The shard list to release.
     * @param readEndShardList The shard list finished.
     * @throws DatahubClientException Throws DatahubClientException
     */
    SyncGroupResult syncGroup(String projectName, String topicName, String consumerGroup, String consumerId, long versionId, List<String> releaseShardList, List<String> readEndShardList);

    /**
     * Leave consumer group info.
     *
     * @param projectName The name of the project.
     * @param topicName The name of the topic.
     * @param consumerGroup The consumerGroup use subId.
     * @param consumerId The consumerId.
     * @param versionId The offset version id.
     * @throws DatahubClientException Throws DatahubClientException
     */
    LeaveGroupResult leaveGroup(String projectName, String topicName, String consumerGroup, String consumerId, long versionId);
}
