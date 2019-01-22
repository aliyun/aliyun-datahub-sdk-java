package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.*;

public class JsonSerializerFactory implements SerializerFactory {
    @Override
    public ErrorParser getErrorParser() throws DatahubClientException {
        return null;
    }

    @Override
    public Serializer<DefaultRequest, CreateProjectRequest> getCreateProjectRequestSer() {
        return CreateProjectRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<CreateProjectResult, CreateProjectRequest, Response> getCreateProjectResultDeser() {
        return CreateProjectResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, DeleteProjectRequest> getDeleteProjectRequestSer() {
        return DeleteProjectRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<DeleteProjectResult, DeleteProjectRequest, Response> getDeleteProjectResultDeser() {
        return DeleteProjectResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, GetProjectRequest> getGetProjectRequestSer() {
        return GetProjectRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<GetProjectResult, GetProjectRequest, Response> getGetProjectResultDeser() {
        return GetProjectResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, ListProjectRequest> getListProjectRequestSer() {
        return ListProjectRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<ListProjectResult, ListProjectRequest, Response> getListProjectResultDeser() {
        return ListProjectResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, CreateTopicRequest> getCreateTopicRequestSer() {
        return CreateTopicRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<CreateTopicResult, CreateTopicRequest, Response> getCreateTopicResultDeser() {
        return CreateTopicResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, DeleteTopicRequest> getDeleteTopicRequestSer() {
        return DeleteTopicRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<DeleteTopicResult, DeleteTopicRequest, Response> getDeleteTopicResultDeser() {
        return DeleteTopicResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, GetTopicRequest> getGetTopicRequestSer() {
        return GetTopicRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<GetTopicResult, GetTopicRequest, Response> getGetTopicResultDeser() {
        return GetTopicResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, ListTopicRequest> getListTopicRequestSer() {
        return ListTopicRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<ListTopicResult, ListTopicRequest, Response> getListTopicResultDeser() {
        return ListTopicResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, UpdateTopicRequest> getUpdateTopicRequestSer() {
        return UpdateTopicRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<UpdateTopicResult, UpdateTopicRequest, Response> getUpdateTopicResultDeser() {
        return UpdateTopicResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, ListShardRequest> getListShardRequestSer() {
        return ListShardRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<ListShardResult, ListShardRequest, Response> getListShardResultDeser() {
        return ListShardResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, SplitShardRequest> getSplitShardRequestSer() {
        return SplitShardRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<SplitShardResult, SplitShardRequest, Response> getSplitShardResultDeser() {
        return SplitShardResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, MergeShardRequest> getMergeShardRequestSer() {
        return MergeShardRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<MergeShardResult, MergeShardRequest, Response> getMergeShardResultDeser() {
        return MergeShardResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, GetCursorRequest> getGetCursorRequestSer() {
        return GetCursorRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<GetCursorResult, GetCursorRequest, Response> getGetCursorResultDeser() {
        return GetCursorResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, GetRecordsRequest> getGetRecordsRequestSer() {
        return GetRecordsRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<GetRecordsResult, GetRecordsRequest, Response> getGetRecordsResultDeser() {
        return GetRecordsResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, PutRecordsRequest> getPutRecordsRequestSer() {
        return PutRecordsRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<PutRecordsResult, PutRecordsRequest, Response> getPutRecordsResultDeser() {
        return PutRecordsResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, ListConnectorRequest> getListConnectorRequestSer() {
        return ListConnectorRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<ListConnectorResult, ListConnectorRequest, Response> getListConnectorResultDeser() {
        return ListConnectorResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, CreateConnectorRequest> getCreateConnectorRequestSer() {
        return CreateConnectorRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<CreateConnectorResult, CreateConnectorRequest, Response> getCreateConnectorResultDeser() {
        return CreateConnectorResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, GetConnectorRequest> getGetConnectorRequestSer() {
        return GetConnectorRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<GetConnectorResult, GetConnectorRequest, Response> getGetConnectorResultDeser() {
        return GetConnectorResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, DeleteConnectorRequest> getDeleteConnectorRequestSer() {
        return DeleteConnectorRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<DeleteConnectorResult, DeleteConnectorRequest, Response> getDeleteConnectorResultDeser() {
        return DeleteConnectorResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, ReloadConnectorRequest> getReloadConnectorRequestSer() {
        return ReloadConnectorRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<ReloadConnectorResult, ReloadConnectorRequest, Response> getReloadConnectorResultDeser() {
        return ReloadConnectorResultJsonDeser.getInstance();
    }
    //

    @Override
    public Serializer<DefaultRequest, GetConnectorShardStatusRequest> getGetConnectorShardStatusRequestSer() {
        return GetConnectorShardStatusRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<GetConnectorShardStatusResult, GetConnectorShardStatusRequest, Response> getGetConnectorShardStatusResultDeser() {
        return GetConnectorShardStatusResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, GetBlobRecordsRequest> getGetBlobRecordsRequestSer() {
        return GetBlobRecordsRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<GetBlobRecordsResult, GetBlobRecordsRequest, Response> getGetBlobRecordsResultDeser() {
        return GetBlobRecordsResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, PutBlobRecordsRequest> getPutBlobRecordsRequestSer() {
        return PutBlobRecordsRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<PutBlobRecordsResult, PutBlobRecordsRequest, Response> getPutBlobRecordsResultDeser() {
        return PutBlobRecordsResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, ExtendShardRequest> getExtendShardRequestSer() {
        return ExtendShardRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<ExtendShardResult, ExtendShardRequest, Response> getExtendShardResultDeser() {
        return ExtendShardResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, AppendFieldRequest> getAppendFieldRequestSer() {
        return AppendFieldRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<AppendFieldResult, AppendFieldRequest, Response> getAppendFieldResultDeser() {
        return AppendFieldResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, AppendConnectorFieldRequest> getAppendConnectorFieldRequestSer() {
        return AppendConnectorFieldRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<AppendConnectorFieldResult, AppendConnectorFieldRequest, Response> getAppendConnectorFieldResultDeser() {
        return AppendConnectorFieldResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, UpdateDataConnectorStateRequest> getUpdateConnectorStateRequestSer() {
        return UpdateDataConnectorStateRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<UpdateDataConnectorStateResult, UpdateDataConnectorStateRequest, Response> getUpdateConnectorStateResultDeser() {
        return UpdateDataConnectorStateResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, UpdateDataConnectorShardContextRequest> getUpdateConnectorShardContextRequestSer() {
        return UpdateDataConnectorShardContextRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<UpdateDataConnectorShardContextResult, UpdateDataConnectorShardContextRequest, Response> getUpdateConnectorShardContextResultDeser() {
        return UpdateDataConnectorShardContextResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, GetMeteringInfoRequest> getGetMeteringInfoRequestSer() {
        return GetMeteringInfoRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<GetMeteringInfoResult, GetMeteringInfoRequest, Response> getGetMeteringInfoResultDeser() {
        return GetMeteringInfoResultJsonDeser.getInstance();
    }


    @Override
    public Serializer<DefaultRequest, ListDataConnectorRequest> getListDataConnectorRequestSer() {
        return ListDataConnectorRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<ListDataConnectorResult, ListDataConnectorRequest, Response> getListDataConnectorResultDeser() {
        return ListDataConnectorResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, CreateDataConnectorRequest> getCreateDataConnectorRequestSer() {
        return CreateDataConnectorRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<CreateDataConnectorResult, CreateDataConnectorRequest, Response> getCreateDataConnectorResultDeser() {
        return CreateDataConnectorResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, GetDataConnectorRequest> getGetDataConnectorRequestSer() {
        return GetDataConnectorRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<GetDataConnectorResult, GetDataConnectorRequest, Response> getGetDataConnectorResultDeser() {
        return GetDataConnectorResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, DeleteDataConnectorRequest> getDeleteDataConnectorRequestSer() {
        return DeleteDataConnectorRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<DeleteDataConnectorResult, DeleteDataConnectorRequest, Response> getDeleteDataConnectorResultDeser() {
        return DeleteDataConnectorResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, ReloadDataConnectorRequest> getReloadDataConnectorRequestSer() {
        return ReloadDataConnectorRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<ReloadDataConnectorResult, ReloadDataConnectorRequest, Response> getReloadDataConnectorResultDeser() {
        return ReloadDataConnectorResultJsonDeser.getInstance();
    }
    //

    @Override
    public Serializer<DefaultRequest, GetDataConnectorShardStatusRequest> getGetDataConnectorShardStatusRequestSer() {
        return GetDataConnectorShardStatusRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<GetDataConnectorShardStatusResult, GetDataConnectorShardStatusRequest, Response> getGetDataConnectorShardStatusResultDeser() {
        return GetDataConnectorShardStatusResultJsonDeser.getInstance();
    }
    
    @Override
    public Serializer<DefaultRequest, AppendDataConnectorFieldRequest> getAppendDataConnectorFieldRequestSer() {
        return AppendDataConnectorFieldRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<AppendDataConnectorFieldResult, AppendDataConnectorFieldRequest, Response> getAppendDataConnectorFieldResultDeser() {
        return AppendDataConnectorFieldResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, CreateSubscriptionRequest> getCreateSubscriptionRequestSer() throws DatahubClientException {
        return CreateSubscriptionRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<CreateSubscriptionResult, CreateSubscriptionRequest, Response> getCreateSubscriptionResultDeser() throws DatahubClientException {
        return CreateSubscriptionResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, DeleteSubscriptionRequest> getDeleteSubscriptionRequestSer() throws DatahubClientException {
        return DeleteSubscriptionRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<DeleteSubscriptionResult, DeleteSubscriptionRequest, Response> getDeleteSubscriptionResultDeser() throws DatahubClientException {
        return DeleteSubscriptionResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, GetOffsetRequest> getGetOffsetRequestSer() throws DatahubClientException {
        return GetOffsetRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<GetOffsetResult, GetOffsetRequest, Response> getGetOffsetResultDeser() throws DatahubClientException {
        return GetOffsetResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, GetSubscriptionRequest> getGetSubscriptionRequestSer() throws DatahubClientException {
        return GetSubscriptionRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<GetSubscriptionResult, GetSubscriptionRequest, Response> getGetSubscriptionResultDeser() throws DatahubClientException {
        return GetSubscriptionResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, QuerySubscriptionRequest> getQuerySubscriptionRequestSer() throws DatahubClientException {
        return QuerySubscriptionRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<QuerySubscriptionResult, QuerySubscriptionRequest, Response> getQuerySubscriptionResultDeser() throws DatahubClientException {
        return QuerySubscriptionResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, CommitOffsetRequest> getCommitOffsetRequestSer() throws DatahubClientException {
        return CommitOffsetRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<CommitOffsetResult, CommitOffsetRequest, Response> getCommitOffsetResultDeser() throws DatahubClientException {
        return CommitOffsetResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, UpdateSubscriptionRequest> getUpdateSubscriptionRequestSer() throws DatahubClientException {
        return UpdateSubscriptionRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<UpdateSubscriptionResult, UpdateSubscriptionRequest, Response> getUpdateSubscriptionResultDerser() throws DatahubClientException {
        return UpdateSubscriptionResultJsonDeser.getInstance();
    }

    @Override
    public Serializer<DefaultRequest, GetDataConnectorDoneTimeRequest> getGetDataConnectorDoneTimeRequestSer() throws DatahubClientException {
        return GetDataConnectorDoneTimeRequestJsonSer.getInstance();
    }

    @Override
    public Deserializer<GetDataConnectorDoneTimeResult, GetDataConnectorDoneTimeRequest, Response> getGetDataConnectorDoneTimeResultDeser() throws DatahubClientException {
        return GetDataConnectorDoneTimeResultJsonDeser.getInstance();
    }

    private JsonSerializerFactory() {
    }

    private static JsonSerializerFactory instance;

    public static JsonSerializerFactory getInstance() {
        if (instance == null) {
            instance = new JsonSerializerFactory();
        }
        return instance;
    }

	@Override
	public Serializer<DefaultRequest, UpdateSubscriptionStateRequest> getUpdateSubscriptionStateRequestSer()
			throws DatahubClientException {
		return UpdateSubscriptionStateRequestJsonSer.getInstance();
	}

	@Override
	public Deserializer<UpdateSubscriptionResult, UpdateSubscriptionStateRequest, Response> getUpdateSubscriptionStateResultDerser()
			throws DatahubClientException {
		return UpdateSubscriptionStateResultJsonDeser.getInstance();
	}

	@Override
	public Serializer<DefaultRequest, InitOffsetContextRequest> getInitOffsetContextRequestSer()
			throws DatahubClientException {
		return InitOffsetContextRequestJsonSer.getInstance();
	}

	@Override
	public Deserializer<InitOffsetContextResult, InitOffsetContextRequest, Response> getInitOffsetContextResultDerser()
			throws DatahubClientException {
		return InitOffsetContextResultJsonDeser.getInstance();
	}

	@Override
	public Serializer<DefaultRequest, ResetOffsetRequest> getResetOffsetRequestSer()
			throws DatahubClientException {
		return ResetOffsetRequestJsonSer.getInstance();
	}

	@Override
	public Deserializer<CommitOffsetResult, ResetOffsetRequest, Response> getResetOffsetResultDeser()
			throws DatahubClientException {
		return ResetOffsetResultJsonDeser.getInstance();
	}
}
