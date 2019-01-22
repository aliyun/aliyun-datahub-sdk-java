package com.aliyun.datahub;

import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.exception.*;
import com.aliyun.datahub.model.*;
import com.aliyun.datahub.model.GetCursorRequest.CursorType;
import com.aliyun.datahub.model.serialize.*;
import com.aliyun.datahub.rest.RestClient;
import com.aliyun.datahub.common.util.KeyRangeUtils;

import java.util.*;

/**
 * Client for accessing DataHub. All service calls made using this client are
 * blocking, and will not return until the service call completes.
 */
public class DatahubClient {

    /**
     * Client configuration
     */
    private DatahubConfiguration conf;
    /**
     * Method requests/results serializer factory
     */
    protected SerializerFactory factory;
    /**
     * Http client
     */
    protected RestClient restClient;
    final private Long MAX_WAITING_MILLISECOND = 120000L;

    /**
     * Construct a new client to invoke service methods on DataHub.
     *
     * All service calls made using this new client object are blocking, and
     * will not return until the service call completes.
     *
     * @param conf The client configuration options.
     */
    public DatahubClient(DatahubConfiguration conf) {
        this.conf = conf;
        this.factory = JsonSerializerFactory.getInstance();
        this.restClient = conf.newRestClient();
    }

    /**
     * Construct a new client to invoke service methods on DataHub.
     *
     * All service calls made using this new client object are blocking, and
     * will not return until the service call completes.
     *
     * @param conf
     *          The client configuration options.
     * @param factory
     *          The method request/result serializer factory.
     */
    public DatahubClient(DatahubConfiguration conf, SerializerFactory factory) {
        this.conf = conf;
        this.factory = factory;
        this.restClient = conf.newRestClient();
    }


    /**
     * refresh the acount of client only for aliyunaccount
     * @param account aliyun account
     */
    public void setAccount(AliyunAccount account) {
        restClient.setAccount(account);
    }

    public String getSourceIpForConsole() {
        return restClient.getSourceIp();
    }

    public void setSourceIpForConsole(String sourceIp) {
        restClient.setSourceIp(sourceIp);
    }

    public boolean getSecureTransportForConsole() {
        return restClient.getSecureTransport();
    }

    public void setSecureTransportForConsole(boolean secureTransport) {
        restClient.setSecureTransport(secureTransport);
    }

    /**
     * Get the specified project information.
     * The information about the project includes its last modified time,
     * its created time and its description set by creator.
     * @param projectName
     *        The name of the project.
     * @return Result of the GetProject operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found. The stream might not
     *         be specified correctly.
     */
    public GetProjectResult getProject(String projectName) {
        return getProject(new GetProjectRequest(projectName));
    }

    /**
     * Get the specified project information.
     * The information about the project includes its last modified time,
     * its created time and its description set by creator.
     * @param request
     *        Represents the input for <code>GetProject</code>.
     * @return Result of the GetProject operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found. The stream might not
     *         be specified correctly.
     */
    public GetProjectResult getProject(GetProjectRequest request) {
        DefaultRequest req = factory.getGetProjectRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getGetProjectResultDeser().deserialize(request, response);
    }

    /**
     * List your projects.
     *
     * @return Result of the ListProject operation returned by the service.
     */
    public ListProjectResult listProject() {
        return listProject(new ListProjectRequest());
    }

    /**
     * List your projects.
     *
     * @param request
     *        Represents the input for <code>ListProject</code>.
     * @return Result of the ListStreams operation returned by the service.
     */
    public ListProjectResult listProject(ListProjectRequest request) {
        DefaultRequest req = factory.getListProjectRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getListProjectResultDeser().deserialize(request, response);
    }

    /**
     * Wait for all shards' status of this topic is ACTIVE.
     * Default timeout value is 30 seconds.
     *
     * @param projectName
     *         The name of the project.
     * @param topicName
     *         The name of the topic.
     */
    public void waitForShardReady(String projectName, String topicName) {
        waitForShardReady(projectName, topicName, 30 * 1000);
    }

    /**
     * Wait for all shards' status of this topic is ACTIVE.
     *
     * @param projectName
     *         The name of the project.
     * @param topicName
     *         The name of the topic.
     * @param timeout
     *         The timeout value of waiting.
     */
    public void waitForShardReady(String projectName, String topicName, long timeout) {
        if (timeout < 0) {
            throw new IllegalArgumentException("invalid timeout value: " + timeout);
        }

        timeout = timeout < MAX_WAITING_MILLISECOND ? timeout : MAX_WAITING_MILLISECOND;

        long now = System.currentTimeMillis();

        long end = now + timeout;

        while (now < end) {
            try {
                if (isShardLoadCompleted(projectName, topicName)) {
                    return;
                }
                Thread.sleep(1000L);
                now = System.currentTimeMillis();
            } catch (Exception e) {
                throw new DatahubClientException("sleep");
            }
        }

        if (!isShardLoadCompleted(projectName, topicName)) {
            throw new DatahubClientException("wait load shard timeout");
        }
    }

    /**
     * 
     * Create a DataHub topic.
     *
     * Server returns response immediately when received the CreateTopic request.
     * All shards are in <code>ACTIVE</code> status after topic created.
     * Write operation can only be performed on <code>ACTIVE</code> shards.
     * 
     * You can use <code>ListShard</code> to get the shard status.
     * 
     * @param projectName
     *        The name of the project.
     * @param topicName
     *        The name of the topic.
     * @param shardCount
     *        The initial shard count of the topic.
     * @param lifeCycle
     *        The data records expired time after which data can not be accessible.
     *        Number of days.
     * @param recordType
     *        The records type of this topic.
     * @param recordSchema
     *        The records schema of this topic.
     * @param desc
     *        The comment of this topic.
     * @throws ResourceExistException
     *         The resource is not available for this operation.
     * @throws ResourceNotFoundException
     *         The project is not created.
     * @throws InvalidParameterException
     *         A specified parameter exceeds its restrictions, is not supported,
     *         or can't be used. For more information, see the returned message.
     */
    public void createTopic(String projectName, String topicName, int shardCount, int lifeCycle, RecordType recordType, RecordSchema recordSchema, String desc) {
        createTopic(new CreateTopicRequest(projectName, topicName, shardCount, lifeCycle, recordType, recordSchema, desc));
    }

    /**
     *
     * Create a DataHub topic.
     *
     * Server returns response immediately when received the CreateTopic request.
     * All shards are in <code>ACTIVE</code> status after topic created.
     * Write operation can only be performed on <code>ACTIVE</code> shards.
     *
     * You can use <code>ListShard</code> to get the shard status.
     *
     * @param projectName
     *        The name of the project.
     * @param topicName
     *        The name of the topic.
     * @param shardCount
     *        The initial shard count of the topic.
     * @param lifeCycle
     *        The data records expired time after which data can not be accessible.
     *        Number of days.
     * @param recordType
     *        The records type of this topic.
     * @param desc
     * @throws ResourceExistException
     *         The resource is not available for this operation.
     * @throws ResourceNotFoundException
     *         The project is not created.
     * @throws InvalidParameterException
     *         A specified parameter exceeds its restrictions, is not supported,
     *         or can't be used. For more information, see the returned message.
     */
    public void createTopic(String projectName, String topicName, int shardCount, int lifeCycle, RecordType recordType, String desc) {
        createTopic(new CreateTopicRequest(projectName, topicName, shardCount, lifeCycle, recordType, null, desc));
    }

    /**
     * 
     * Create a DataHub topic.
     * 
     * Server returns response immediately when received the CreateTopic request.
     * All shards are in <code>ACTIVE</code> status after topic created.
     * Write operation can only be performed on <code>ACTIVE</code> shards.
     * 
     * You can use <code>ListShard</code> to get the shard status.
     * 
     * @param request
     *        Represents the input for <code>CreateTopic</code>.
     * @throws ResourceExistException
     *         The topic is already created.
     * @throws ResourceNotFoundException
     *         The project is not created.
     * @throws InvalidParameterException
     *         A specified parameter exceeds its restrictions, is not supported,
     *         or can't be used. For more information, see the returned message.
     */
    public void createTopic(CreateTopicRequest request) {
        DefaultRequest req = factory.getCreateTopicRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        factory.getCreateTopicResultDeser().deserialize(request, response);
    }

    /**
     * Delete a topic and all its shards and data.
     *
     * @param projectName
     *        The name of the project.
     * @param topicName
     *        The name of the topic.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found. The name might not
     *         be specified correctly.
     */
    public void deleteTopic(String projectName, String topicName) {
        deleteTopic(new DeleteTopicRequest(projectName, topicName));
    }

    /**
     * Delete a topic and all its shards and data.
     *
     * @param request
     *        Represents the input for <a>DeleteStream</a>.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found. The name might not
     *         be specified correctly.
     */
    public void deleteTopic(DeleteTopicRequest request) {
        DefaultRequest req = factory.getDeleteTopicRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        factory.getDeleteTopicResultDeser().deserialize(request, response);
    }

    /**
     * Update properties for the specified topic.
     *
     * @param projectName
     *        The name of the project.
     * @param topicName
     *        The name of the topic.
     * @param lifeCycle
     *        The data records expired time after which data can not be accessible.
     *        Unit of time represents in day.
     * @param desc
     *        The description of the topic overwrite by caller.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws InvalidParameterException
     *         A specified parameter exceeds its restrictions, is not supported,
     *         or can't be used. For more information, see the returned message.
     */
    public UpdateTopicResult updateTopic(String projectName, String topicName, int lifeCycle, String desc) {
        return updateTopic(new UpdateTopicRequest(projectName, topicName, lifeCycle, desc));
    }

    /**
     * Update properties for the specified topic.
     *
     * @param request
     *        Represents the input for <code>UpdateTopic</code>.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws InvalidParameterException
     *         A specified parameter exceeds its restrictions, is not supported,
     *         or can't be used. For more information, see the returned message.
     */
    public UpdateTopicResult updateTopic(UpdateTopicRequest request) {
        DefaultRequest req = factory.getUpdateTopicRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getUpdateTopicResultDeser().deserialize(request, response);
    }

    /**
     * 
     * Get the specified topic information.
     * 
     * 
     * The information about the topic includes its shard count, its lifecycle,
     * its record type, its record schema, its created time, its last modified
     * time and its description.
     * 
     * @param projectName
     *        The name of project.
     * @param topicName
     *        The name of topic.
     * @return Result of the GetTopic operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     */
    public GetTopicResult getTopic(String projectName, String topicName) {
        return getTopic(new GetTopicRequest(projectName, topicName));
    }

    /**
     * 
     * Get the specified topic information.
     * 
     * 
     * The information about the topic includes its shard count, its lifecycle,
     * its record type, its record schema, its created time, its last modified
     * time and its description.
     * 
     * @param request
     *        Represents the input for <code>GetTopic</code>.
     * @return Result of the GetTopic operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     */
    public GetTopicResult getTopic(GetTopicRequest request) {
        DefaultRequest req = factory.getGetTopicRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getGetTopicResultDeser().deserialize(request, response);
    }

    /**
     * List your topics.
     *
     * @param projectName
     *         The name of the project.
     * @return Result of the ListTopic operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     */
    public ListTopicResult listTopic(String projectName) {
        return listTopic(new ListTopicRequest(projectName));
    }

    /**
     * List your topics.
     *
     * @param request
     *        Represents the input for <code>ListTopic</code>.
     * @return Result of the ListTopic operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     */
    public ListTopicResult listTopic(ListTopicRequest request) {
        DefaultRequest req = factory.getListTopicRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getListTopicResultDeser().deserialize(request, response);
    }

    /**
     * List shards.
     *
     * @param projectName
     *        The name of the project.
     * @param topicName
     *        The name of the topic.
     * @return Result of the ListShard operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     */
    public ListShardResult listShard(String projectName, String topicName) {
        return listShard(new ListShardRequest(projectName, topicName));
    }

    /**
     * List shards.
     *
     * @param request
     *        Represents the input for <code>ListShard</code>.
     * @return Result of the ListShard operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     */
    public ListShardResult listShard(ListShardRequest request) {
        DefaultRequest req = factory.getListShardRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getListShardResultDeser().deserialize(request, response);
    }

    /**
     * Trivial Split shards.
     *
     * @param projectName
     *        The name of the project.
     * @param topicName
     *        The name of the topic.
     * @return Result of the SplitShard operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws InvalidOperationException
     *         The requested shard is not active.
     * @throws LimitExceededException
     *         Request is too frequent or exceed max shard number.
     */
    public SplitShardResult splitShard(String projectName, String topicName, String shardId) {
        String splitKey = null;
        ListShardResult resp = listShard(projectName, topicName);
        for (ShardEntry entry : resp.getShards()) {
            if (shardId.equals(entry.getShardId())) {
                splitKey = KeyRangeUtils.trivialSplit(entry.getBeginHashKey(), entry.getEndHashKey());
                break;
            }
        }
        if (splitKey == null) {
            throw new DatahubClientException("Shard Not Exists.");
        }
        return splitShard(new SplitShardRequest(projectName, topicName, shardId, splitKey));
    }

    /**
     * Split shards.
     *
     * @param projectName
     *        The name of the project.
     * @param topicName
     *        The name of the topic.
     * @param shardId
     *        The shard to split.
     * @param splitKey
     *        The split key.
     * @return Result of the SplitShard operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws InvalidOperationException
     *         The requested shard is not active.
     * @throws LimitExceededException
     *         Request is too frequent or exceed max shard number.
     */
    public SplitShardResult splitShard(String projectName, String topicName, String shardId, String splitKey) {
        return splitShard(new SplitShardRequest(projectName, topicName, shardId, splitKey));
    }

    /**
     * Split shards.
     *
     * @param request
     *        Represents the input for <code>SplitShard</code>.
     * @return Result of the SplitShard operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws InvalidOperationException
     *         The requested shard is not active.
     * @throws LimitExceededException
     *         Request is too frequent or exceed max shard number.
     */
    public SplitShardResult splitShard(SplitShardRequest request) {
        DefaultRequest req = factory.getSplitShardRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getSplitShardResultDeser().deserialize(request, response);
    }

    /**
     * Merge shards.
     *
     * @param projectName
     *        The name of the project.
     * @param topicName
     *        The name of the topic.
     * @param shardId
     *        The shard to merge.
     * @param adjacentShardId
     *        The adjacentShard to be merged.
     * @return Result of the MergeShard operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws InvalidOperationException
     *         The requested shard is not active.
     * @throws LimitExceededException
     *         Request is too frequent or exceed max shard number.
     */
    public MergeShardResult mergeShard(String projectName, String topicName, String shardId, String adjacentShardId) {
        return mergeShard(new MergeShardRequest(projectName, topicName, shardId, adjacentShardId));
    }

    /**
     * Merge shards.
     *
     * @param request
     *        Represents the input for <code>MergeShard</code>.
     * @return Result of the MergeShard operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws InvalidOperationException
     *         The requested shard is not active.
     * @throws LimitExceededException
     *         Request is too frequent or exceed max shard number.
     */
    public MergeShardResult mergeShard(MergeShardRequest request) {
        DefaultRequest req = factory.getMergeShardRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getMergeShardResultDeser().deserialize(request, response);
    }

    /**
     * Get a shard cursor.
     * 
     * 
     * A shard cursor specifies the position in the shard from which to start
     * reading data records sequentially.
     * 
     * 
     * You must specify the shard cursor type. For example, you can set the
     * <code>CursorType</code> to start reading at the last untrimmed record
     * in the shard in the system, which is the oldest data record in the shard
     * by using the <code>OLDEST</code> cursor type. You can specify the shard
     * cursor type <code>LATEST</code> in the request to start reading just after
     * the most recent record in the shard.
     * 
     * 
     * Or you must specify the <code>time</code> to start reading records which
     * received at the most recent time after that point.
     * 
     * @param request Represents the input for <code>GetCursor</code>.
     * @return Result of the GetCursor operation returned by the service.
     * @throws ResourceNotFoundException The requested resource could not be found.
     * @throws InvalidParameterException A specified parameter exceeds its restrictions, is not supported,
     *                                   or can't be used. For more information, see the returned message.
     */
    public GetCursorResult getCursor(GetCursorRequest request) {
        DefaultRequest req = factory.getGetCursorRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getGetCursorResultDeser().deserialize(request, response);
    }

    /**
     * Get a shard cursor.
     *
     * @param projectName
     *         The name of the project.
     * @param topicName
     *         The name of topic.
     * @param shardId
     *         The shard ID of shard.
     * @param timestamp
     *         start reading records which received at the most recent time after 'timestamp',
     *         represents as unix epoch in millisecond.
     * @return Result of the GetCursor operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws InvalidParameterException
     *         A specified parameter exceeds its restrictions, is not supported,
     *         or can't be used. For more information, see the returned message.
     */
    public GetCursorResult getCursor(String projectName, String topicName, String shardId, long timestamp) {
        GetCursorRequest request = new GetCursorRequest(projectName, topicName, shardId, timestamp);
        return getCursor(request);
    }

    /**
     * Get a shard cursor.
     *
     * @param projectName
     *         The name of the project.
     * @param topicName
     *         The name of topic.
     * @param shardId
     *         The shard ID of shard.
     * @param type
     *         The cursor type.
     * @return Result of the GetCursor operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws InvalidParameterException
     *         A specified parameter exceeds its restrictions, is not supported,
     *         or can't be used. For more information, see the returned message.
     */
    public GetCursorResult getCursor(String projectName, String topicName, String shardId, GetCursorRequest.CursorType type) {
        GetCursorRequest request = new GetCursorRequest(projectName, topicName, shardId, type);
        return getCursor(request);
    }

    /**
     * Get a shard cursor.
     *
     * @param projectName
     *         The name of the project.
     * @param topicName
     *         The name of topic.
     * @param shardId
     *         The shard ID of shard.
     * @param type
     *         The cursor type.
     * @param param
     *         Parameter of type
     * @return Result of the GetCursor operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws InvalidParameterException
     *         A specified parameter exceeds its restrictions, is not supported,
     *         or can't be used. For more information, see the returned message.
     */
    public GetCursorResult getCursor(String projectName, String topicName, String shardId, GetCursorRequest.CursorType type, long param) {
        GetCursorRequest request = new GetCursorRequest(projectName, topicName, shardId, type, param);
        return getCursor(request);
    }
    
    /**
     * Get next offset cursor.
     *
     * @param offsetCtx The Offset Context
     * @return Result of the GetCursor operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws InvalidParameterException
     *         A specified parameter exceeds its restrictions, is not supported,
     *         or can't be used. For more information, see the returned message.
     */
    public GetCursorResult getNextOffsetCursor(OffsetContext offsetCtx) {
    	GetCursorRequest request = new GetCursorRequest(offsetCtx.getProject(), offsetCtx.getTopic(), offsetCtx.getShardId(), CursorType.SEQUENCE,
				(offsetCtx.getOffset().getSequence() + 1));
    	return getCursor(request);
    }

    /**
     * Get current offset cursor.
     *
     * @param offsetCtx The Offset Context
     * @return Result of the GetCursor operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws InvalidParameterException
     *         A specified parameter exceeds its restrictions, is not supported,
     *         or can't be used. For more information, see the returned message.
     */
    public GetCursorResult getCurrentOffsetCursor(OffsetContext offsetCtx) {
    	GetCursorRequest request = new GetCursorRequest(offsetCtx.getProject(), offsetCtx.getTopic(), offsetCtx.getShardId(), CursorType.SEQUENCE,
				(offsetCtx.getOffset().getSequence()));
    	return getCursor(request);
    }

    /**
     * 
     * Get data records from a shard.
     * 
     * 
     * Specify a shard cursor using the <code>Cursor</code> parameter.
     * The shard cursor specifies the position in the shard from which you
     * want to start reading data records sequentially. If there are no records
     * available in the portion of the shard that the cursor points to,
     * <a>GetRecords</a> returns an empty list.
     * 
     * @param request
     *         Represents the input for <a>GetRecords</a>.
     * @return Result of the GetRecords operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws InvalidParameterException
     *         A specified parameter exceeds its restrictions, is not supported,
     *         or can't be used. For more information, see the returned message.
     * @throws MalformedRecordException
     *         The data records received is malformed, or conflicts with the specified schema.
     */
    public GetRecordsResult getRecords(GetRecordsRequest request) {
        DefaultRequest req = factory.getGetRecordsRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getGetRecordsResultDeser().deserialize(request, response);
    }

    /**
     * Get data records from a shard.
     *
     * @param projectName
     *         The name of the project.
     * @param topicName
     *         The name of topic.
     * @param shardId
     *         The shard ID of shard.
     * @param cursor
     *         The value of cursor.
     * @param limit
     *         The maximum number of records returned by service.
     * @return Result of the GetRecords operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws InvalidParameterException
     *         A specified parameter exceeds its restrictions, is not supported,
     *         or can't be used. For more information, see the returned message.
     * @throws MalformedRecordException
     *         The data records received is malformed, or conflicts with the specified schema.
     */
    public GetRecordsResult getRecords(String projectName, String topicName, String shardId, String cursor, int limit, RecordSchema schema) {
        GetRecordsRequest request = new GetRecordsRequest(projectName, topicName, shardId, cursor, limit);
        request.setSchema(schema);
        return getRecords(request);
    }

    /**
     * Get blob data records from a shard.
     *
     * @param projectName
     *         The name of the project.
     * @param topicName
     *         The name of topic.
     * @param shardId
     *         The shard ID of shard.
     * @param cursor
     *         The value of cursor.
     * @param limit
     *         The maximum number of records returned by service.
     * @return Result of the GetRecords operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws InvalidParameterException
     *         A specified parameter exceeds its restrictions, is not supported,
     *         or can't be used. For more information, see the returned message.
     * @throws MalformedRecordException
     *         The data records received is malformed, or conflicts with the specified schema.
     */
    public GetBlobRecordsResult getBlobRecords(String projectName, String topicName, String shardId, String cursor, int limit) {
        GetBlobRecordsRequest request = new GetBlobRecordsRequest(projectName, topicName, shardId, cursor, limit);
        return getBlobRecords(request);
    }

    /**
     *
     * Get blob data records from a shard.
     *
     *
     * Specify a shard cursor using the <code>Cursor</code> parameter.
     * The shard cursor specifies the position in the shard from which you
     * want to start reading data records sequentially. If there are no records
     * available in the portion of the shard that the cursor points to,
     * <a>GetRecords</a> returns an empty list.
     *
     * @param request
     *         Represents the input for <a>GetBlobRecords</a>.
     * @return Result of the GetBlobRecords operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws InvalidParameterException
     *         A specified parameter exceeds its restrictions, is not supported,
     *         or can't be used. For more information, see the returned message.
     * @throws MalformedRecordException
     *         The data records received is malformed, or conflicts with the specified schema.
     */
    public GetBlobRecordsResult getBlobRecords(GetBlobRecordsRequest request) {
        DefaultRequest req = factory.getGetBlobRecordsRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getGetBlobRecordsResultDeser().deserialize(request, response);
    }

    public PutRecordsResult putRecords(String projectName, String topicName, List<RecordEntry> entries, int retries) {
        PutRecordsResult result = putRecords(projectName, topicName, entries);
        for (int i = 0; i < retries; ++i) {
            if (result.getFailedRecordCount() == 0) {
                return result;
            } else {
                result = putRecords(projectName, topicName, result.getFailedRecords());
            }
        }
        return result;
    }

    /**
     * 
     * Write data records into a DataHub topic.
     * 
     * 
     * The response includes unsuccessfully processed records. DataHub attempts to
     * process all records in each <code>PutRecords</code> request. A single record
     * failure does not stop the processing of subsequent records.
     * 
     * 
     * An unsuccessfully-processed record includes <code>ErrorCode</code> and
     * <code>ErrorMessage</code> values.
     * 
     * @param projectName
     *        The name of the project.
     * @param topicName
     *        The name of the topic.
     * @param entries
     *        The array of records sent to service.
     * @return Result of the PutRecords operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found. The topic might not
     *         be specified correctly.
     * @throws InvalidParameterException
     *         A specified parameter exceeds its restrictions, is not supported,
     *         or can't be used. For more information, see the returned message.
     */
    public PutRecordsResult putRecords(String projectName, String topicName, List<RecordEntry> entries) {
        return putRecords(new PutRecordsRequest(projectName, topicName, entries));
    }

    /**
     * 
     * Write data records into a DataHub topic.
     * 
     * 
     * The response includes unsuccessfully processed records. DataHub attempts to
     * process all records in each <code>PutRecords</code> request. A single record
     * failure does not stop the processing of subsequent records.
     * 
     * 
     * An unsuccessfully-processed record includes <code>ErrorCode</code> and
     * <code>ErrorMessage</code> values.
     * 
     * @param request
     *        A <code>PutRecords</code> request.
     * @return Result of the PutRecords operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found. The topic might not
     *         be specified correctly.
     * @throws InvalidParameterException
     *         A specified parameter exceeds its restrictions, is not supported,
     *         or can't be used. For more information, see the returned message.
     */
    public PutRecordsResult putRecords(PutRecordsRequest request) {
        DefaultRequest req = factory.getPutRecordsRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        PutRecordsResult rs = factory.getPutRecordsResultDeser().deserialize(request, response);
        List<RecordEntry> records = request.getRecords();
        for (int i : rs.getFailedRecordIndex()) {
            rs.addFailedRecord(records.get(i));
        }
        return rs;
    }

    public PutBlobRecordsResult putBlobRecords(String projectName, String topicName, List<BlobRecordEntry> entries, int retries) {
        PutBlobRecordsResult result = putBlobRecords(projectName, topicName, entries);
        for (int i = 0; i < retries; ++i) {
            if (result.getFailedRecordCount() == 0) {
                return result;
            } else {
                result = putBlobRecords(projectName, topicName, result.getFailedRecords());
            }
        }
        return result;
    }

    /**
     *
     * Write blob data records into a DataHub topic.
     *
     *
     * The response includes unsuccessfully processed records. DataHub attempts to
     * process all records in each <code>PutRecords</code> request. A single record
     * failure does not stop the processing of subsequent records.
     *
     *
     * An unsuccessfully-processed record includes <code>ErrorCode</code> and
     * <code>ErrorMessage</code> values.
     *
     * @param projectName
     *        The name of the project.
     * @param topicName
     *        The name of the topic.
     * @param entries
     *        The array of records sent to service.
     * @return Result of the PutBlobRecords operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found. The topic might not
     *         be specified correctly.
     * @throws InvalidParameterException
     *         A specified parameter exceeds its restrictions, is not supported,
     *         or can't be used. For more information, see the returned message.
     */
    public PutBlobRecordsResult putBlobRecords(String projectName, String topicName, List<BlobRecordEntry> entries) {
        return putBlobRecords(new PutBlobRecordsRequest(projectName, topicName, entries));
    }

    /**
     *
     * Write blob data records into a DataHub topic.
     *
     *
     * The response includes unsuccessfully processed records. DataHub attempts to
     * process all records in each <code>PutRecords</code> request. A single record
     * failure does not stop the processing of subsequent records.
     *
     *
     * An unsuccessfully-processed record includes <code>ErrorCode</code> and
     * <code>ErrorMessage</code> values.
     *
     * @param request
     *        A <code>PutBlobRecords</code> request.
     * @return Result of the PutBlobRecords operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found. The topic might not
     *         be specified correctly.
     * @throws InvalidParameterException
     *         A specified parameter exceeds its restrictions, is not supported,
     *         or can't be used. For more information, see the returned message.
     */
    public PutBlobRecordsResult putBlobRecords(PutBlobRecordsRequest request) {
        DefaultRequest req = factory.getPutBlobRecordsRequestSer().serialize(request);
        Response response = this.restClient.requestWithNoRetry(req);
        PutBlobRecordsResult rs = factory.getPutBlobRecordsResultDeser().deserialize(request, response);
        List<BlobRecordEntry> records = request.getRecords();
        for (int i : rs.getFailedRecordIndex()) {
            rs.addFailedRecord(records.get(i));
        }
        return rs;
    }

    /**
     * Create a Datahub project.
     * The concept of project is used to serve multiple tenants.
     *
     * @param projectName
     *         The name of the project.
     * @param desc
     *         The description of the project.
     * @throws ResourceExistException
     *         The project is already created.
     * @throws InvalidParameterException
     *         A specified parameter exceeds its restrictions, is not supported,
     *         or can't be used. For more information, see the returned message.
     */
    public void createProject(String projectName, String desc) {
        createProject(new CreateProjectRequest(projectName, desc));
    }

    /**
     * Create a Datahub project.
     * The concept of project is used to serve multiple tenants.
     *
     * @param request Represents the input for <code>CreateProject</code>.
     * @throws ResourceExistException
     *         The project is already created.
     * @throws InvalidParameterException
     *         A specified parameter exceeds its restrictions, is not supported,
     *         or can't be used. For more information, see the returned message.
     */
    public void createProject(CreateProjectRequest request) {
        DefaultRequest req = factory.getCreateProjectRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        factory.getCreateProjectResultDeser().deserialize(request, response);
    }

    /**
     * Delete a project and all its topics and data.
     *
     * @param projectName
     *        The name of the project.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found. The project might not
     *         be specified correctly.
     */
    public void deleteProject(String projectName) {
        deleteProject(new DeleteProjectRequest(projectName));
    }

    /**
     * Delete a project and all its topics and data.
     *
     * @param request
     *        Represents the input for <a>DeleteProject</a>.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found. The project might not
     *         be specified correctly.
     */
    public void deleteProject(DeleteProjectRequest request) {
        DefaultRequest req = factory.getDeleteProjectRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        factory.getDeleteProjectResultDeser().deserialize(request, response);
    }

    /**
     * Append field.
     *
     * @param request
     *        Represents the input for <code>appendField</code>.
     * @return Result of the appendField operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws InvalidParameterException
     *         The requested shard number is invalid.
     */
    public AppendFieldResult appendField(AppendFieldRequest request) {
        DefaultRequest req = factory.getAppendFieldRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getAppendFieldResultDeser().deserialize(request, response);
    }

    /**
     * Get specific shard metering info.
     *
     * @param request
     *        Represents the input for <code>getMeteringInfo</code>.
     * @return Result of the getMeteringInfo operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws InvalidParameterException
     *         The requested shard number is invalid.
     */
    public GetMeteringInfoResult getMeteringInfo(GetMeteringInfoRequest request) {
        DefaultRequest req = factory.getGetMeteringInfoRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getGetMeteringInfoResultDeser().deserialize(request, response);
    }

    /**
     * List your data connectors.
     *
     * @param projectName
     *         The name of the project.
     * @param topicName
     *         The name of the topic.
     * @return Result of the ListDataConnector operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     */
    public ListDataConnectorResult listDataConnector(String projectName, String topicName) {
        return listDataConnector(new ListDataConnectorRequest(projectName, topicName));
    }

    /**
     * List your data connectors.
     *
     * @param request
     *        Represents the input for <code>ListDataConnector</code>.
     * @return Result of the ListConnector operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     */
    public ListDataConnectorResult listDataConnector(ListDataConnectorRequest request) {
        DefaultRequest req = factory.getListDataConnectorRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getListDataConnectorResultDeser().deserialize(request, response);
    }

    /**
     * Create data connectors.
     *
     * @param projectName
     *         The name of the project.
     * @param topicName
     *         The name of the topic.
     * @param connectorType
     *         The type of connector.
     * @param columnFields
     *         The list of the columnFields.
     * @param config
     *         The config map of the connector.
     * @return Result of the CreateDataConnector operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws ResourceExistException
     *         The project is already created.
     */
    public CreateDataConnectorResult createDataConnector(String projectName, String topicName,
                                                 ConnectorType connectorType, List<String> columnFields, ConnectorConfig config) {
        return createDataConnector(new CreateDataConnectorRequest(projectName, topicName, connectorType, columnFields, config));
    }

    public CreateDataConnectorResult createDataConnector(String projectName, String topicName,
                                                         ConnectorType connectorType, ConnectorConfig config) {
        return createDataConnector(new CreateDataConnectorRequest(projectName, topicName, connectorType, config));
    }

    /**
     * Create data connectors.
     *
     * @param request
     *        Represents the input for <code>CreateDataConnector</code>.
     * @return Result of the CreateDataConnector operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws ResourceExistException
     *         The project is already created.
     */
    public CreateDataConnectorResult createDataConnector(CreateDataConnectorRequest request) {
        DefaultRequest req = factory.getCreateDataConnectorRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getCreateDataConnectorResultDeser().deserialize(request, response);
    }

    /**
     * Get data connectors.
     *
     * @param projectName
     *         The name of the project.
     * @param topicName
     *         The name of the topic.
     * @param connectorType
     *         The type of connector.
     * @return Result of the GetDataConnector operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     */
    public GetDataConnectorResult getDataConnector(String projectName, String topicName, ConnectorType connectorType) {
        return getDataConnector(new GetDataConnectorRequest(projectName, topicName, connectorType));
    }

    /**
     * Get your data connectors.
     *
     * @param request
     *        Represents the input for <code>GetDataConnector</code>.
     * @return Result of the GetDataConnector operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     */
    public GetDataConnectorResult getDataConnector(GetDataConnectorRequest request) {
        DefaultRequest req = factory.getGetDataConnectorRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getGetDataConnectorResultDeser().deserialize(request, response);
    }

    /**
     * Get your data connectors doneTime.
     *
     * @param request
     *        Represents the input for <code>GetDataConnectorDoneTime</code>.
     * @return Result of the GetDataConnectorDoneTime operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     */
    public GetDataConnectorDoneTimeResult getDataConnectorDoneTime(GetDataConnectorDoneTimeRequest request) {
        DefaultRequest req = factory.getGetDataConnectorDoneTimeRequestSer().serialize(request);


        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getGetDataConnectorDoneTimeResultDeser().deserialize(request, response);
    }

    /**
     * Delete data connectors.
     *
     * @param projectName
     *         The name of the project.
     * @param topicName
     *         The name of the topic.
     * @param connectorType
     *         The type of the connector.
     * @return Result of the DeleteConnector operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     */
    public DeleteDataConnectorResult deleteDataConnector(String projectName, String topicName, ConnectorType connectorType) {
        return deleteDataConnector(new DeleteDataConnectorRequest(projectName, topicName, connectorType));
    }

    /**
     * Delete data connectors.
     *
     * @param request
     *        Represents the input for <code>DeleteDataConnector</code>.
     * @return Result of the DeleteDataConnector operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     */
    public DeleteDataConnectorResult deleteDataConnector(DeleteDataConnectorRequest request) {
        DefaultRequest req = factory.getDeleteDataConnectorRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getDeleteDataConnectorResultDeser().deserialize(request, response);
    }

    /**
     * Reload data connectors.
     *
     * @param request
     *        Represents the input for <code>ReloadDataConnector</code>.
     * @return Result of the ReloadDataConnector operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     */
    public ReloadDataConnectorResult reloadDataConnector(ReloadDataConnectorRequest request) {
        DefaultRequest req = factory.getReloadDataConnectorRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getReloadDataConnectorResultDeser().deserialize(request, response);
    }

    /**
     * Reload data connectors.
     *
     * @param projectName
     *         The name of the project.
     * @param topicName
     *         The name of the topic.
     * @param connectorType
     *         The type of the connector.
     * @param shardId
     *         The shard id.
     * @return Result of the ReloadDataConnector operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     */
    public ReloadDataConnectorResult reloadDataConnector(String projectName, String topicName, ConnectorType connectorType, String shardId) {
        return reloadDataConnector(new ReloadDataConnectorRequest(projectName, topicName, connectorType, shardId));
    }

    /**
     * Reload all shard for data connectors.
     *
     * @param projectName
     *         The name of the project.
     * @param topicName
     *         The name of the topic.
     * @param connectorType
     *         The type of the connector.
     * @return Result of the ReloadDataConnector operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     */
    public ReloadDataConnectorResult reloadDataConnector(String projectName, String topicName, ConnectorType connectorType) {
        return reloadDataConnector(new ReloadDataConnectorRequest(projectName, topicName, connectorType));
    }

    /**
     * Get data connector shard status.
     *
     * @param projectName
     *         The name of the project.
     * @param topicName
     *         The name of the topic.
     * @param connectorType
     *         The type of the connector.
     * @param shardId
     *         The shard id.
     * @return Result of the GetDataConnectorShardStatus operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     */
    public GetDataConnectorShardStatusResult getDataConnectorShardStatus(String projectName, String topicName, ConnectorType connectorType, String shardId) {
        return getDataConnectorShardStatus(new GetDataConnectorShardStatusRequest(projectName, topicName, connectorType, shardId));
    }

    /**
     * Get data connector shard status.
     *
     * @param request
     *        Represents the input for <code>GetDataConnectorShardStatus</code>.
     * @return Result of the GetConnectorShardStatus operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     */
    public GetDataConnectorShardStatusResult getDataConnectorShardStatus(GetDataConnectorShardStatusRequest request) {
        DefaultRequest req = factory.getGetDataConnectorShardStatusRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getGetDataConnectorShardStatusResultDeser().deserialize(request, response);
    }

    /**
     * Append data connector field.
     *
     * @param request
     *        Represents the input for <code>appendDataConnectorField</code>.
     * @return Result of the appendConnectorField operation returned by the service.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found.
     * @throws InvalidParameterException
     *         The requested shard number is invalid.
     */
    public AppendDataConnectorFieldResult appendDataConnectorField(AppendDataConnectorFieldRequest request) {
        DefaultRequest req = factory.getAppendDataConnectorFieldRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getAppendDataConnectorFieldResultDeser().deserialize(request, response);
    }
    
    /**
     * Initial Offset Context
     * 
     * @param project
     *        The name of the project
     * @param topic
     *        The name of the topic
     * @param subId
     *        The id of the subscription
     * @param shardIds
     *        The set of shard ids
     * @return
     *        Result returned by service
     * @throws InvalidParameterException
     *        Some of the parameters are invalid
     * @throws InternalFailureException
     *        The service encountered an error
     */
    public InitOffsetContextResult initOffsetContext(String project, String topic, String subId, Set<String> shardIds) {
    	return initOffsetContext(new InitOffsetContextRequest(project, topic, subId, shardIds));
    }
    
    /**
     * Initial Offset Context 
     * 
     * @param project
     *        The name of the project
     * @param topic
     *        The name of the topic
     * @param subId
     *        The id of the subscription
     * @param shardId
     *        The set of shard id
     * @return
     *        Result returned by service
     * @throws InvalidParameterException
     *        Some of the parameters are invalid
     * @throws InternalFailureException
     *        The service encountered an error
     */    
    public OffsetContext initOffsetContext(String project, String topic, String subId, String shardId) {
    	Set<String> shardIds = new HashSet<String>();
    	shardIds.add(shardId);
    	InitOffsetContextResult result = initOffsetContext(new InitOffsetContextRequest(project, topic, subId, shardIds));
    	return result.getOffsets().get(shardId);
    }
    
    /**
     * Initial Offset Context
     * 
     * @param request
     * @return
     *        Result returned by service
     * @throws InvalidParameterException
     *        Some of the parameters are invalid
     * @throws InternalFailureException
     *        The service encountered an error
     */
    public InitOffsetContextResult initOffsetContext(InitOffsetContextRequest request) {
        DefaultRequest req = factory.getInitOffsetContextRequestSer().serialize(request);
        
        Response response = this.restClient.requestWithNoRetry(req);
        
        return factory.getInitOffsetContextResultDerser().deserialize(request, response);
    }
    
    /**
     * Update offset context
     * 
     * @param offsetCtx
     *        The current offset context
     *
     */
    public void updateOffsetContext(OffsetContext offsetCtx) {
    	Set<String> shardIds = new HashSet<String>();
    	shardIds.add(offsetCtx.getShardId());
    	GetOffsetRequest request = new GetOffsetRequest(offsetCtx.getProject(), offsetCtx.getTopic(), offsetCtx.getSubId(), shardIds);
        DefaultRequest req = factory.getGetOffsetRequestSer().serialize(request);
        Response response = this.restClient.requestWithNoRetry(req);
        GetOffsetResult result = factory.getGetOffsetResultDeser().deserialize(request, response);
    	offsetCtx.setOffset(result.getOffsets().get(offsetCtx.getShardId()));
    	offsetCtx.setVersion(result.getVersions().get(offsetCtx.getShardId()));
    }
    
    /**
     * Commit offset
     * @param offsetCtx
     *        offset context
     * @return
     *        Result returned by service
     * @throws InvalidParameterException
     *        Some of the parameters are invalid
     * @throws InternalFailureException
     *        The service encountered an error
     */
    public CommitOffsetResult commitOffset(OffsetContext offsetCtx) {
    	Map<String, OffsetContext> offsetCtxMap = new HashMap<String, OffsetContext>();
    	offsetCtxMap.put(offsetCtx.getShardId(), offsetCtx);
        return commitOffset(new CommitOffsetRequest(offsetCtxMap));
    }
    
    public CommitOffsetResult commitOffset(Map<String, OffsetContext> offsetCtxMap) {
    	return commitOffset(new CommitOffsetRequest(offsetCtxMap));
    }

    /**
     * Commit subscription offset
     * @param request
     *        Request with project name, sub id, offset list
     * @return
     *        Result returned by service
     * @throws InvalidParameterException
     *        Some of the parameters are invalid
     * @throws InternalFailureException
     *        The service encountered an error
     */
    public CommitOffsetResult commitOffset(CommitOffsetRequest request) {
        DefaultRequest req = factory.getCommitOffsetRequestSer().serialize(request);

        Response response = this.restClient.requestWithNoRetry(req);

        return factory.getCommitOffsetResultDeser().deserialize(request, response);
    }

    /**
     * clear resources (connection pool)
     */
    public void close() {
        restClient.close();
    }

    private boolean isShardLoadCompleted(String projectName, String topicName) {
        try {
            ListShardResult result = listShard(projectName, topicName);
            List<ShardEntry> shards = result.getShards();
            for (ShardEntry shard : shards) {
                if (shard.getState() != ShardState.ACTIVE && shard.getState() != ShardState.CLOSED) {
                    return false;
                }
            }

            return true;
        } catch (Exception e) {
        }

        return false;
    }
}
