package com.aliyun.datahub;

import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.http.interceptor.InterceptorWrapper;
import com.aliyun.datahub.client.impl.AbstractDatahubClient;
import com.aliyun.datahub.client.impl.interceptor.DatahubAuthInterceptor;
import com.aliyun.datahub.client.impl.interceptor.DatahubResponseInterceptor;
import com.aliyun.datahub.client.model.SinkConfig;
import com.aliyun.datahub.client.model.SubscriptionOffset;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.exception.*;
import com.aliyun.datahub.model.*;
import com.aliyun.datahub.model.serialize.SerializerFactory;
import com.aliyun.datahub.utils.ModelConvertToNew;

import java.util.*;
import java.util.concurrent.Callable;

import static com.aliyun.datahub.client.common.ErrorCode.*;

/**
 * @deprecated As of release new sdk, replaced by {@link com.aliyun.datahub.client.DatahubClient}
 */
@Deprecated
public class DatahubClient {
    private com.aliyun.datahub.client.DatahubClient proxyClient;

    /**
     * Http client
     */
//    protected RestClient restClient;
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
        AliyunAccount aliyunAccount = (AliyunAccount)conf.getAccount();

        // init restClient for other call compatible
//        restClient = conf.newRestClient();

        DatahubConfig datahubConfig = new DatahubConfig(conf.getEndpoint(),
                new com.aliyun.datahub.client.auth.AliyunAccount(
                    aliyunAccount != null ? aliyunAccount.getAccessId() : null,
                    aliyunAccount != null ? aliyunAccount.getAccessKey() : null,
                    aliyunAccount != null ? aliyunAccount.getSecurityToken() : null
                ), conf.isEnableBinary());

        HttpConfig httpConfig = new HttpConfig()
                .setConnTimeout(conf.getSocketConnectTimeout() * 1000)
                .setReadTimeout(conf.getSocketTimeout() * 1000)
                .setProxyUri(conf.getProxyUri())
                .setProxyUsername(conf.getProxyUsername())
                .setProxyPassword(conf.getProxyPassword())
                .setCompressType(conf.getCompressionFormat() == null ?
                        null : HttpConfig.CompressType.valueOf(conf.getCompressionFormat().toString().toUpperCase()));

        proxyClient = DatahubClientBuilder.newBuilder()
                .setDatahubConfig(datahubConfig)
                .setHttpConfig(httpConfig)
                .build();
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
        this(conf);
    }


    /**
     * refresh the acount of client only for aliyunaccount
     * @param account aliyun account
     */
    public void setAccount(com.aliyun.datahub.auth.AliyunAccount account) {
        InterceptorWrapper interceptor = new InterceptorWrapper()
                .setAuth(new DatahubAuthInterceptor(new com.aliyun.datahub.client.auth.AliyunAccount(account.getAccessId(), account.getAccessKey(), account.getSecurityToken()), null))
                .setResponse(new DatahubResponseInterceptor());

        ((AbstractDatahubClient)proxyClient).innerSetInterceptor(interceptor);
    }

    public String getSourceIpForConsole() {
        return null;
    }

    public void setSourceIpForConsole(String sourceIp) {
    }

    public boolean getSecureTransportForConsole() {
        return false;
    }

    public void setSecureTransportForConsole(boolean secureTransport) {
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
    public GetProjectResult getProject(final String projectName) {
        return callWrapper(new Callable<GetProjectResult>() {
            @Override
            public GetProjectResult call() throws Exception {
                return new GetProjectResult(proxyClient.getProject(projectName));
            }
        });
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
        return getProject(request.getProjectName());
    }

    /**
     * List your projects.
     *
     * @return Result of the ListProject operation returned by the service.
     */
    public ListProjectResult listProject() {
        return callWrapper(new Callable<ListProjectResult>() {
            @Override
            public ListProjectResult call() throws Exception {
                return new ListProjectResult(proxyClient.listProject());
            }
        });
    }

    /**
     * List your projects.
     *
     * @param request
     *        Represents the input for <code>ListProject</code>.
     * @return Result of the ListStreams operation returned by the service.
     */
    public ListProjectResult listProject(ListProjectRequest request) {
        return listProject();
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
     *         or can't be used. For more information, see the returned compress.
     */
    public CreateTopicResult createTopic(final String projectName, final String topicName, final int shardCount, final int lifeCycle, final RecordType recordType, RecordSchema recordSchema, final String desc) {
        // convert record schema

        final com.aliyun.datahub.client.model.RecordSchema newRecordSchema = recordSchema == null ? null : ModelConvertToNew.convertRecordSchema(recordSchema);

        return callWrapper(new Callable<CreateTopicResult>() {
            @Override
            public CreateTopicResult call() throws Exception {
                return new CreateTopicResult(proxyClient.createTopic(projectName, topicName, shardCount, lifeCycle,
                        recordType == RecordType.TUPLE ? com.aliyun.datahub.client.model.RecordType.TUPLE : com.aliyun.datahub.client.model.RecordType.BLOB,
                        newRecordSchema, desc));
            }
        });
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
     *         or can't be used. For more information, see the returned compress.
     */
    public CreateTopicResult createTopic(final String projectName, final String topicName, final int shardCount, final int lifeCycle, final RecordType recordType, final String desc) {
        // convert record schema
        return callWrapper(new Callable<CreateTopicResult>() {
            @Override
            public CreateTopicResult call() throws Exception {
                return new CreateTopicResult(proxyClient.createTopic(projectName, topicName, shardCount, lifeCycle,
                        recordType == RecordType.TUPLE ? com.aliyun.datahub.client.model.RecordType.TUPLE : com.aliyun.datahub.client.model.RecordType.BLOB, desc));
            }
        });
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
     *         or can't be used. For more information, see the returned compress.
     */
    public CreateTopicResult createTopic(CreateTopicRequest request) {
        if (request.getRecordSchema() == null) {
            return createTopic(
                request.getProjectName(),
                request.getTopicName(),
                request.getShardCount(),
                request.getLifeCycle(),
                request.getRecordType(),
                request.getComment()
            );
        } else {
            return createTopic(
                    request.getProjectName(),
                    request.getTopicName(),
                    request.getShardCount(),
                    request.getLifeCycle(),
                    request.getRecordType(),
                    request.getRecordSchema(),
                    request.getComment()
            );
        }
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
    public DeleteTopicResult deleteTopic(final String projectName, final String topicName) {
        return callWrapper(new Callable<DeleteTopicResult>() {
            @Override
            public DeleteTopicResult call() throws Exception {
                return new DeleteTopicResult(proxyClient.deleteTopic(projectName, topicName));
            }
        });
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
    public DeleteTopicResult deleteTopic(DeleteTopicRequest request) {
        return deleteTopic(request.getProjectName(), request.getTopicName());
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
     *         or can't be used. For more information, see the returned compress.
     */
    public UpdateTopicResult updateTopic(final String projectName, final String topicName, final int lifeCycle, final String desc) {
        return callWrapper(new Callable<UpdateTopicResult>() {
            @Override
            public UpdateTopicResult call() throws Exception {
                return new UpdateTopicResult(proxyClient.updateTopic(projectName, topicName, desc));
            }
        });
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
     *         or can't be used. For more information, see the returned compress.
     */
    public UpdateTopicResult updateTopic(UpdateTopicRequest request) {
        return updateTopic(request.getProjectName(),
                request.getTopicName(),
                request.getLifeCycle(),
                request.getComment());
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
    public GetTopicResult getTopic(final String projectName, final String topicName) {
        return callWrapper(new Callable<GetTopicResult>() {
            @Override
            public GetTopicResult call() throws Exception {
                return new GetTopicResult(proxyClient.getTopic(projectName, topicName));
            }
        });
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
        return getTopic(request.getProjectName(), request.getTopicName());
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
    public ListTopicResult listTopic(final String projectName) {
        return callWrapper(new Callable<ListTopicResult>() {
            @Override
            public ListTopicResult call() throws Exception {
                return new ListTopicResult(proxyClient.listTopic(projectName));
            }
        });
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
        return listTopic(request.getProjectName());
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
    public ListShardResult listShard(final String projectName, final String topicName) {
        return callWrapper(new Callable<ListShardResult>() {
            @Override
            public ListShardResult call() throws Exception {
                return new ListShardResult(proxyClient.listShard(projectName, topicName));
            }
        });
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
        return listShard(request.getProjectName(), request.getTopicName());
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
    public SplitShardResult splitShard(final String projectName, final String topicName, final String shardId) {
        return callWrapper(new Callable<SplitShardResult>() {
            @Override
            public SplitShardResult call() throws Exception {
                return new SplitShardResult(proxyClient.splitShard(projectName, topicName, shardId));
            }
        });
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
    public SplitShardResult splitShard(final String projectName, final String topicName, final String shardId, final String splitKey) {
        return callWrapper(new Callable<SplitShardResult>() {
            @Override
            public SplitShardResult call() throws Exception {
                return new SplitShardResult(proxyClient.splitShard(projectName, topicName, shardId, splitKey));
            }
        });
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
        if (request.getSplitKey() != null) {
            return splitShard(request.getProjectName(), request.getTopicName(), request.getShardId(), request.getSplitKey());
        }
        return splitShard(request.getProjectName(), request.getTopicName(), request.getShardId());
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
    public MergeShardResult mergeShard(final String projectName, final String topicName, final String shardId, final String adjacentShardId) {
        return callWrapper(new Callable<MergeShardResult>() {
            @Override
            public MergeShardResult call() throws Exception {
                return new MergeShardResult(proxyClient.mergeShard(projectName, topicName, shardId, adjacentShardId));
            }
        });
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
        return mergeShard(request.getProjectName(),
                request.getTopicName(),
                request.getShardId(),
                request.getAdjacentShardId());
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
     *                                   or can't be used. For more information, see the returned compress.
     */
    public GetCursorResult getCursor(GetCursorRequest request) {
        long param = request.getType() != null && request.getType() == GetCursorRequest.CursorType.SEQUENCE ?
                request.getSequence() : request.getTimestamp();

        return getCursor(request.getProjectName(),
                request.getTopicName(),
                request.getShardId(),
                request.getType(),
                param);
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
     *         or can't be used. For more information, see the returned compress.
     */
    public GetCursorResult getCursor(String projectName, String topicName, String shardId, long timestamp) {
        return getCursor(projectName, topicName, shardId, GetCursorRequest.CursorType.SYSTEM_TIME, timestamp);
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
     *         or can't be used. For more information, see the returned compress.
     */
    public GetCursorResult getCursor(String projectName, String topicName, String shardId, GetCursorRequest.CursorType type) {
        return getCursor(projectName, topicName, shardId, type, -1);
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
     *         or can't be used. For more information, see the returned compress.
     */
    public GetCursorResult getCursor(final String projectName, final String topicName, final String shardId, final GetCursorRequest.CursorType type, final long param) {
        return callWrapper(new Callable<GetCursorResult>() {
            @Override
            public GetCursorResult call() throws Exception {
                return new GetCursorResult(proxyClient.getCursor(projectName, topicName, shardId,
                        com.aliyun.datahub.client.model.CursorType.valueOf(type.name().toUpperCase()), param));
            }
        });
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
     *         or can't be used. For more information, see the returned compress.
     */
    public GetCursorResult getNextOffsetCursor(OffsetContext offsetCtx) {
    	GetCursorRequest request = new GetCursorRequest(offsetCtx.getProject(), offsetCtx.getTopic(), offsetCtx.getShardId(), GetCursorRequest.CursorType.SEQUENCE,
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
     *         or can't be used. For more information, see the returned compress.
     */
    public GetCursorResult getCurrentOffsetCursor(OffsetContext offsetCtx) {
    	GetCursorRequest request = new GetCursorRequest(offsetCtx.getProject(), offsetCtx.getTopic(), offsetCtx.getShardId(), GetCursorRequest.CursorType.SEQUENCE,
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
     *         or can't be used. For more information, see the returned compress.
     * @throws MalformedRecordException
     *         The data records received is malformed, or conflicts with the specified schema.
     */
    public GetRecordsResult getRecords(GetRecordsRequest request) {
        return getRecords(request.getProjectName(),
                request.getTopicName(),
                request.getShardId(),
                request.getCursor(),
                request.getLimit(),
                request.getSchema());
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
     *         or can't be used. For more information, see the returned compress.
     * @throws MalformedRecordException
     *         The data records received is malformed, or conflicts with the specified schema.
     */
    public GetRecordsResult getRecords(final String projectName, final String topicName, final String shardId, final String cursor, final int limit, RecordSchema schema) {
        final com.aliyun.datahub.client.model.RecordSchema newSchema = schema == null ? null : ModelConvertToNew.convertRecordSchema(schema);

        return callWrapper(new Callable<GetRecordsResult>() {
            @Override
            public GetRecordsResult call() throws Exception {
                return new GetRecordsResult(proxyClient.getRecords(projectName, topicName, shardId, newSchema, cursor, limit));
            }
        });
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
     *         or can't be used. For more information, see the returned compress.
     * @throws MalformedRecordException
     *         The data records received is malformed, or conflicts with the specified schema.
     */
    public GetBlobRecordsResult getBlobRecords(final String projectName, final String topicName, final String shardId, final String cursor, final int limit) {
        return callWrapper(new Callable<GetBlobRecordsResult>() {
            @Override
            public GetBlobRecordsResult call() throws Exception {
                return new GetBlobRecordsResult(proxyClient.getRecords(projectName, topicName, shardId, cursor, limit));
            }
        });
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
     *         or can't be used. For more information, see the returned compress.
     * @throws MalformedRecordException
     *         The data records received is malformed, or conflicts with the specified schema.
     */
    public GetBlobRecordsResult getBlobRecords(GetBlobRecordsRequest request) {
        return getBlobRecords(request.getProjectName(),
                request.getTopicName(),
                request.getShardId(),
                request.getCursor(),
                request.getLimit());
    }

    public PutRecordsResult putRecords(String projectName, String topicName, List<RecordEntry> entries, int retries) {
        return putRecords(projectName, topicName, entries);
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
     *         or can't be used. For more information, see the returned compress.
     */
    public PutRecordsResult putRecords(final String projectName, final String topicName, final List<RecordEntry> entries) {
        final List<com.aliyun.datahub.client.model.RecordEntry> newRecordEntries = new ArrayList<>();
        return callWrapper(new Callable<PutRecordsResult>() {
            @Override
            public PutRecordsResult call() throws Exception {

                for (RecordEntry entry : entries) {
                    newRecordEntries.add(ModelConvertToNew.convertRecordEntry(entry));
                }

                return new PutRecordsResult(proxyClient.putRecords(projectName, topicName, newRecordEntries));
            }
        });
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
     *         or can't be used. For more information, see the returned compress.
     */
    public PutRecordsResult putRecords(PutRecordsRequest request) {
        return putRecords(request.getProjectName(),
                request.getTopicName(),
                request.getRecords());
    }

    public PutBlobRecordsResult putBlobRecords(String projectName, String topicName, List<BlobRecordEntry> entries, int retries) {
        return putBlobRecords(projectName, topicName, entries);
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
     *         or can't be used. For more information, see the returned compress.
     */
    public PutBlobRecordsResult putBlobRecords(final String projectName, final String topicName, List<BlobRecordEntry> entries) {
        final List<com.aliyun.datahub.client.model.RecordEntry> newEntryList = new ArrayList<>();
        for (BlobRecordEntry entry : entries) {
            newEntryList.add(ModelConvertToNew.convertBlobRecordEntry(entry));
        }

        return callWrapper(new Callable<PutBlobRecordsResult>() {
            @Override
            public PutBlobRecordsResult call() throws Exception {
                return new PutBlobRecordsResult(proxyClient.putRecords(projectName, topicName, newEntryList));
            }
        });
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
     *         or can't be used. For more information, see the returned compress.
     */
    public PutBlobRecordsResult putBlobRecords(PutBlobRecordsRequest request) {
        return putBlobRecords(request.getProjectName(),
                request.getTopicName(),
                request.getRecords());
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
     *         or can't be used. For more information, see the returned compress.
     */
    public CreateProjectResult createProject(final String projectName, final String desc) {
        return callWrapper(new Callable<CreateProjectResult>() {
            @Override
            public CreateProjectResult call() throws Exception {
                return new CreateProjectResult(proxyClient.createProject(projectName, desc));
            }
        });
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
     *         or can't be used. For more information, see the returned compress.
     */
    public CreateProjectResult createProject(CreateProjectRequest request) {
        return createProject(request.getProjectName(), request.getComment());
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
    public DeleteProjectResult deleteProject(final String projectName) {
        return callWrapper(new Callable<DeleteProjectResult>() {
            @Override
            public DeleteProjectResult call() throws Exception {
                return new DeleteProjectResult(proxyClient.deleteProject(projectName));
            }
        });
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
    public DeleteProjectResult deleteProject(DeleteProjectRequest request) {
        return deleteProject(request.getProjectName());
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
    public AppendFieldResult appendField(final AppendFieldRequest request) {
        Field field = request.getField();
        com.aliyun.datahub.client.model.FieldType newType = com.aliyun.datahub.client.model.FieldType.valueOf(field.getType().name().toUpperCase());
        final com.aliyun.datahub.client.model.Field newField = new com.aliyun.datahub.client.model.Field(field.getName(), newType, !field.getNotnull());

        return callWrapper(new Callable<AppendFieldResult>() {
            @Override
            public AppendFieldResult call() throws Exception {
                return new AppendFieldResult(proxyClient.appendField(request.getProjectName(), request.getTopicName(), newField));
            }
        });
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
    public GetMeteringInfoResult getMeteringInfo(final GetMeteringInfoRequest request) {
        return callWrapper(new Callable<GetMeteringInfoResult>() {
            @Override
            public GetMeteringInfoResult call() throws Exception {
                return new GetMeteringInfoResult(proxyClient.getMeterInfo(request.getProjectName(), request.getTopicName(), request.getShardId()));
            }
        });
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
    public ListDataConnectorResult listDataConnector(final String projectName, final String topicName) {
        return callWrapper(new Callable<ListDataConnectorResult>() {
            @Override
            public ListDataConnectorResult call() throws Exception {
                return new ListDataConnectorResult(proxyClient.listConnector(projectName, topicName));
            }
        });
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
        return listDataConnector(request.getProjectName(), request.getTopicName());
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
    public CreateDataConnectorResult createDataConnector(final String projectName, final String topicName,
                                                         final ConnectorType connectorType, final List<String> columnFields, ConnectorConfig config) {
        final SinkConfig newConfig = ModelConvertToNew.convertConnectorConfig(config);
        final com.aliyun.datahub.client.model.ConnectorType newType = com.aliyun.datahub.client.model.ConnectorType.valueOf(connectorType.name().toUpperCase());
        return callWrapper(new Callable<CreateDataConnectorResult>() {
            @Override
            public CreateDataConnectorResult call() throws Exception {
                return new CreateDataConnectorResult(proxyClient.createConnector(projectName, topicName, newType, columnFields == null ? new ArrayList<String>() : columnFields, newConfig));
            }
        });
    }

    public CreateDataConnectorResult createDataConnector(String projectName, String topicName,
                                                         ConnectorType connectorType, ConnectorConfig config) {
        return createDataConnector(projectName, topicName, connectorType, null, config);
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
        return createDataConnector(request.getProjectName(),
                request.getTopicName(),
                request.getType(),
                request.getColumnFields(),
                request.getConfig());
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
    public GetDataConnectorResult getDataConnector(final String projectName, final String topicName, ConnectorType connectorType) {
        final com.aliyun.datahub.client.model.ConnectorType newType = com.aliyun.datahub.client.model.ConnectorType.valueOf(connectorType.name().toUpperCase());

        return callWrapper(new Callable<GetDataConnectorResult>() {
            @Override
            public GetDataConnectorResult call() throws Exception {
                return new GetDataConnectorResult(proxyClient.getConnector(projectName, topicName, newType));
            }
        });
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
        return getDataConnector(request.getProjectName(), request.getTopicName(), request.getConnectorType());
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
    public GetDataConnectorDoneTimeResult getDataConnectorDoneTime(final GetDataConnectorDoneTimeRequest request) {
        final com.aliyun.datahub.client.model.ConnectorType newType = com.aliyun.datahub.client.model.ConnectorType.valueOf(request.getConnectorType().name().toUpperCase());

        return callWrapper(new Callable<GetDataConnectorDoneTimeResult>() {
            @Override
            public GetDataConnectorDoneTimeResult call() throws Exception {
                return new GetDataConnectorDoneTimeResult(proxyClient.getConnectorDoneTime(request.getProjectName(),
                        request.getTopicName(), newType));
            }
        });
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
    public DeleteDataConnectorResult deleteDataConnector(final String projectName, final String topicName, ConnectorType connectorType) {
        final com.aliyun.datahub.client.model.ConnectorType newType = com.aliyun.datahub.client.model.ConnectorType.valueOf(connectorType.name().toUpperCase());

        return callWrapper(new Callable<DeleteDataConnectorResult>() {
            @Override
            public DeleteDataConnectorResult call() throws Exception {
                return new DeleteDataConnectorResult(proxyClient.deleteConnector(projectName, topicName, newType));
            }
        });
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
        return deleteDataConnector(request.getProjectName(), request.getTopicName(), request.getConnectorType());
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
    public ReloadDataConnectorResult reloadDataConnector(final ReloadDataConnectorRequest request) {
        return reloadDataConnector(request.getProjectName(), request.getTopicName(), request.getConnectorType(), request.getShardId());
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
    public ReloadDataConnectorResult reloadDataConnector(final String projectName, final String topicName, ConnectorType connectorType, final String shardId) {
        final com.aliyun.datahub.client.model.ConnectorType newType = com.aliyun.datahub.client.model.ConnectorType.valueOf(connectorType.name().toUpperCase());

        return callWrapper(new Callable<ReloadDataConnectorResult>() {
            @Override
            public ReloadDataConnectorResult call() throws Exception {
                return new ReloadDataConnectorResult(proxyClient.reloadConnector(projectName, topicName, newType, shardId));
            }
        });
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
        return reloadDataConnector(projectName, topicName, connectorType, null);
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
    public GetDataConnectorShardStatusResult getDataConnectorShardStatus(final GetDataConnectorShardStatusRequest request) {
        final com.aliyun.datahub.client.model.ConnectorType newType =
                com.aliyun.datahub.client.model.ConnectorType.valueOf(request.getConnectorType().name().toUpperCase());

        return callWrapper(new Callable<GetDataConnectorShardStatusResult>() {
            @Override
            public GetDataConnectorShardStatusResult call() throws Exception {
                return new GetDataConnectorShardStatusResult(request.getShardId(), proxyClient.getConnectorShardStatus(request.getProjectName(),
                        request.getTopicName(), newType));
            }
        });
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
    public AppendDataConnectorFieldResult appendDataConnectorField(final AppendDataConnectorFieldRequest request) {
        final com.aliyun.datahub.client.model.ConnectorType newType =
                com.aliyun.datahub.client.model.ConnectorType.valueOf(request.getConnectorType().name().toUpperCase());

        return callWrapper(new Callable<AppendDataConnectorFieldResult>() {
            @Override
            public AppendDataConnectorFieldResult call() throws Exception {
                return new AppendDataConnectorFieldResult(proxyClient.appendConnectorField(request.getProjectName(), request.getTopicName(), newType, request.getFieldName()));
            }
        });
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
    public InitOffsetContextResult initOffsetContext(final String project, final String topic, final String subId, Set<String> shardIds) {
        final List<String> shardIdList = new ArrayList<>(shardIds);

        return callWrapper(new Callable<InitOffsetContextResult>() {
            @Override
            public InitOffsetContextResult call() throws Exception {
                com.aliyun.datahub.client.model.OpenSubscriptionSessionResult initResult =
                        proxyClient.openSubscriptionSession(project, topic, subId, shardIdList);

                return new InitOffsetContextResult(project, topic, subId, initResult);
            }
        });
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
    	InitOffsetContextResult result = initOffsetContext(project, topic, subId, shardIds);
    	return result.getOffsets().get(shardId);
    }
    
    /**
     * Initial Offset Context
     * 
     * @return
     *        Result returned by service
     * @throws InvalidParameterException
     *        Some of the parameters are invalid
     * @throws InternalFailureException
     *        The service encountered an error
     */
    public InitOffsetContextResult initOffsetContext(InitOffsetContextRequest request) {
        return initOffsetContext(request.getProjectName(),
                request.getTopicName(),
                request.getSubId(),
                request.getShardIds());
    }
    
    /**
     * Update offset context
     * 
     * @param offsetCtx
     *        The current offset context
     *
     */
    public void updateOffsetContext(final OffsetContext offsetCtx) {
        final List<String> shardIds = new ArrayList<>();
        shardIds.add(offsetCtx.getShardId());

        com.aliyun.datahub.client.model.GetSubscriptionOffsetResult offsetResult =
                callWrapper(new Callable<com.aliyun.datahub.client.model.GetSubscriptionOffsetResult>() {
                    @Override
                    public com.aliyun.datahub.client.model.GetSubscriptionOffsetResult call() throws Exception {
                        return proxyClient.getSubscriptionOffset(offsetCtx.getProject(), offsetCtx.getTopic(), offsetCtx.getSubId(), shardIds);
                    }
                });

        SubscriptionOffset subOffset = offsetResult.getOffsets().get(offsetCtx.getShardId());
        offsetCtx.setOffset(new OffsetContext.Offset(subOffset.getSequence(), subOffset.getTimestamp()));
        offsetCtx.setVersion(subOffset.getVersionId());
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
    	return commitOffset(offsetCtxMap);
    }
    
    public CommitOffsetResult commitOffset(Map<String, OffsetContext> offsetCtxMap) {
        if (offsetCtxMap == null) {
            return null;
        }

        OffsetContext offsetCtx = offsetCtxMap.values().iterator().next();
        final String proejctName = offsetCtx.getProject();
        final String topicName = offsetCtx.getTopic();
        final String subId = offsetCtx.getSubId();

        final Map<String, com.aliyun.datahub.client.model.SubscriptionOffset> subscriptionOffsetMap = new HashMap<>();
        for (Map.Entry<String, OffsetContext> entry : offsetCtxMap.entrySet()) {
            SubscriptionOffset subOffset = ModelConvertToNew.convertOffsetContext(entry.getValue());
            subscriptionOffsetMap.put(entry.getKey(), subOffset);
        }

        return callWrapper(new Callable<CommitOffsetResult>() {
            @Override
            public CommitOffsetResult call() throws Exception {
                return new CommitOffsetResult(proxyClient.commitSubscriptionOffset(proejctName, topicName, subId, subscriptionOffsetMap));
            }
        });
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
        return commitOffset(request.getOffsetCtxMap());
    }

    /**
     * clear resources (connection pool)
     */
    public void close() {
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


    // convert com.aliyun.datahub.client.exception to com.aliyun.datahub.exception
    private <T> T callWrapper(Callable<T> callable) {
        try {
            return callable.call();
        } catch (com.aliyun.datahub.client.exception.DatahubClientException ex) {
            if (ex.getErrorCode() != null) {
                checkErrorCodeAndThrow(ex);
            } else {
                checkTypeAndThrow(ex);
            }
        } catch (ResourceNotFoundException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DatahubClientException(ex.getMessage());
        }

        // should never here
        return null;
    }

    private void checkTypeAndThrow(com.aliyun.datahub.client.exception.DatahubClientException ex) {
        DatahubClientException retEx = null;

        // As new sdk adds some validations on the client side, it will throw the specific exceptions.
        // These exceptions thrown by the client not contains ErrorCode.
        // Here we check the type of the exception for compatibility.

        if (ex instanceof com.aliyun.datahub.client.exception.InvalidParameterException) {
            retEx = new InvalidParameterException(ex.getErrorMessage());
        } else if (ex instanceof com.aliyun.datahub.client.exception.MalformedRecordException) {
            retEx = new MalformedRecordException(ex.getErrorMessage());
        } else if (ex instanceof com.aliyun.datahub.client.exception.NoPermissionException) {
            // Only when ErrorCode is null, then throw InvalidOperationException.
            // Mainly used when split shard.
            retEx = new InvalidOperationException(ex.getErrorMessage());
        } else {
            retEx = new DatahubClientException(ex.getMessage());
        }

        if (retEx instanceof DatahubServiceException) {
            ((DatahubServiceException)retEx).setErrorCode(ex.getErrorCode());
            ((DatahubServiceException)retEx).setStatusCode(ex.getHttpStatus());
            ((DatahubServiceException)retEx).setRequestId(ex.getRequestId());
        }

        throw retEx;
    }


    private void checkErrorCodeAndThrow(com.aliyun.datahub.client.exception.DatahubClientException ex) {
        String errCode = ex.getErrorCode();
        DatahubServiceException retEx = null;

        // parse errorCode
        if (INVALID_PARAMETER.equalsIgnoreCase(errCode) ||
                INVALID_SUBSCRIPTION.equalsIgnoreCase(errCode)) {
            retEx = new InvalidParameterException(ex.getErrorMessage());
        } else if (INVALID_CURSOR.equalsIgnoreCase(errCode)) {
            retEx = new InvalidCursorException(ex.getErrorMessage());
        } else if (RESOURCE_NOT_FOUND.equalsIgnoreCase(errCode) ||
                NO_SUCH_PROJECT.equalsIgnoreCase(errCode) ||
                NO_SUCH_TOPIC.equalsIgnoreCase(errCode) ||
                NO_SUCH_CONNECTOR.equalsIgnoreCase(errCode) ||
                NO_SUCH_SHARD.equalsIgnoreCase(errCode) ||
                NO_SUCH_SUBSCRIPTION.equalsIgnoreCase(errCode) ||
                NO_SUCH_METER_INFO.equalsIgnoreCase(errCode)) {
            retEx = new ResourceNotFoundException(ex.getErrorMessage());
        } else if (PROJECT_ALREADY_EXIST.equalsIgnoreCase(errCode) ||
                TOPIC_ALREADY_EXIST.equalsIgnoreCase(errCode) ||
                CONNECTOR_ALREADY_EXIST.equalsIgnoreCase(errCode)) {
            retEx = new ResourceExistException(ex.getErrorMessage());
        } else if (UN_AUTHORIZED.equalsIgnoreCase(errCode)) {
            retEx = new AuthorizationFailureException(ex.getErrorMessage());
        } else if (NO_PERMISSION.equalsIgnoreCase(errCode)) {
            retEx = new NoPermissionException(ex.getErrorMessage());
        } else if (INVALID_SHARD_OPERATION.equalsIgnoreCase(errCode)) {
            retEx = new InvalidOperationException(ex.getErrorMessage());
        } else if (OPERATOR_DENIED.equalsIgnoreCase(errCode)) {
            retEx = new OperationDeniedException(ex.getErrorMessage());
        } else if (LIMIT_EXCEED.equalsIgnoreCase(errCode)) {
            retEx = new LimitExceededException(ex.getErrorMessage());
        } else if (ODPS_SERVICE_ERROR.equalsIgnoreCase(errCode)) {
            retEx = new OdpsException(ex.getErrorMessage());
        } else if (MYSQL_SERVICE_ERROR.equalsIgnoreCase(errCode)) {
            retEx = new MysqlException(ex.getErrorMessage());
        } else if (INTERNAL_SERVER_ERROR.equalsIgnoreCase(errCode)) {
            retEx = new InternalFailureException(ex.getErrorMessage());
        } else if (SUBSCRIPTION_OFFLINE.equalsIgnoreCase(errCode)) {
            retEx = new SubscriptionOfflineException(ex.getErrorMessage());
        } else if (OFFSET_RESETED.equalsIgnoreCase(errCode)) {
            retEx = new OffsetResetedException(ex.getErrorMessage());
        } else if (OFFSET_SESSION_CLOSED.equalsIgnoreCase(errCode)) {
            retEx = new OffsetSessionClosedException(ex.getErrorMessage());
        } else if (OFFSET_SESSION_CHANGED.equalsIgnoreCase(errCode)) {
            retEx = new OffsetSessionChangedException(ex.getErrorMessage());
        } else {
            retEx = new DatahubServiceException(ex.getErrorMessage());
        }

        retEx.setErrorCode(errCode);
        retEx.setStatusCode(ex.getHttpStatus());
        retEx.setRequestId(ex.getRequestId());
        throw retEx;
    }
}
