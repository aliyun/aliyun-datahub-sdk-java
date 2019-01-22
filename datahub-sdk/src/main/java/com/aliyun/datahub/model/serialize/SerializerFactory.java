package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.*;

public interface SerializerFactory {
    public ErrorParser getErrorParser() throws DatahubClientException;

    public Serializer<DefaultRequest, CreateProjectRequest> getCreateProjectRequestSer() throws DatahubClientException;

    public Deserializer<CreateProjectResult, CreateProjectRequest, Response> getCreateProjectResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, DeleteProjectRequest> getDeleteProjectRequestSer() throws DatahubClientException;

    public Deserializer<DeleteProjectResult, DeleteProjectRequest, Response> getDeleteProjectResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, GetProjectRequest> getGetProjectRequestSer() throws DatahubClientException;

    public Deserializer<GetProjectResult, GetProjectRequest, Response> getGetProjectResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, ListProjectRequest> getListProjectRequestSer() throws DatahubClientException;

    public Deserializer<ListProjectResult, ListProjectRequest, Response> getListProjectResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, CreateTopicRequest> getCreateTopicRequestSer() throws DatahubClientException;

    public Deserializer<CreateTopicResult, CreateTopicRequest, Response> getCreateTopicResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, DeleteTopicRequest> getDeleteTopicRequestSer() throws DatahubClientException;

    public Deserializer<DeleteTopicResult, DeleteTopicRequest, Response> getDeleteTopicResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, GetTopicRequest> getGetTopicRequestSer() throws DatahubClientException;

    public Deserializer<GetTopicResult, GetTopicRequest, Response> getGetTopicResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, ListTopicRequest> getListTopicRequestSer() throws DatahubClientException;

    public Deserializer<ListTopicResult, ListTopicRequest, Response> getListTopicResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, UpdateTopicRequest> getUpdateTopicRequestSer() throws DatahubClientException;

    public Deserializer<UpdateTopicResult, UpdateTopicRequest, Response> getUpdateTopicResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, ListShardRequest> getListShardRequestSer() throws DatahubClientException;

    public Deserializer<ListShardResult, ListShardRequest, Response> getListShardResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, GetCursorRequest> getGetCursorRequestSer() throws DatahubClientException;

    public Deserializer<GetCursorResult, GetCursorRequest, Response> getGetCursorResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, GetRecordsRequest> getGetRecordsRequestSer() throws DatahubClientException;

    public Deserializer<GetRecordsResult, GetRecordsRequest, Response> getGetRecordsResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, PutRecordsRequest> getPutRecordsRequestSer() throws DatahubClientException;

    public Deserializer<PutRecordsResult, PutRecordsRequest, Response> getPutRecordsResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, SplitShardRequest> getSplitShardRequestSer() throws DatahubClientException;

    public Deserializer<SplitShardResult, SplitShardRequest, Response> getSplitShardResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, MergeShardRequest> getMergeShardRequestSer() throws DatahubClientException;

    public Deserializer<MergeShardResult, MergeShardRequest, Response> getMergeShardResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, ListConnectorRequest> getListConnectorRequestSer() throws DatahubClientException;

    public Deserializer<ListConnectorResult, ListConnectorRequest, Response> getListConnectorResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, CreateConnectorRequest> getCreateConnectorRequestSer() throws DatahubClientException;

    public Deserializer<CreateConnectorResult, CreateConnectorRequest, Response> getCreateConnectorResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, GetConnectorRequest> getGetConnectorRequestSer() throws DatahubClientException;

    public Deserializer<GetConnectorResult, GetConnectorRequest, Response> getGetConnectorResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, DeleteConnectorRequest> getDeleteConnectorRequestSer() throws DatahubClientException;

    public Deserializer<DeleteConnectorResult, DeleteConnectorRequest, Response> getDeleteConnectorResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, ReloadConnectorRequest> getReloadConnectorRequestSer() throws DatahubClientException;

    public Deserializer<ReloadConnectorResult, ReloadConnectorRequest, Response> getReloadConnectorResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, GetConnectorShardStatusRequest> getGetConnectorShardStatusRequestSer() throws DatahubClientException;

    public Deserializer<GetConnectorShardStatusResult, GetConnectorShardStatusRequest, Response> getGetConnectorShardStatusResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, GetBlobRecordsRequest> getGetBlobRecordsRequestSer() throws DatahubClientException;

    public Deserializer<GetBlobRecordsResult, GetBlobRecordsRequest, Response> getGetBlobRecordsResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, PutBlobRecordsRequest> getPutBlobRecordsRequestSer() throws DatahubClientException;

    public Deserializer<PutBlobRecordsResult, PutBlobRecordsRequest, Response> getPutBlobRecordsResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, ExtendShardRequest> getExtendShardRequestSer() throws DatahubClientException;

    public Deserializer<ExtendShardResult, ExtendShardRequest, Response> getExtendShardResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, AppendFieldRequest> getAppendFieldRequestSer() throws DatahubClientException;

    public Deserializer<AppendFieldResult, AppendFieldRequest, Response> getAppendFieldResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, AppendConnectorFieldRequest> getAppendConnectorFieldRequestSer() throws DatahubClientException;

    public Deserializer<AppendConnectorFieldResult, AppendConnectorFieldRequest, Response> getAppendConnectorFieldResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, UpdateDataConnectorStateRequest> getUpdateConnectorStateRequestSer() throws DatahubClientException;

    public Deserializer<UpdateDataConnectorStateResult, UpdateDataConnectorStateRequest, Response> getUpdateConnectorStateResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, UpdateDataConnectorShardContextRequest> getUpdateConnectorShardContextRequestSer() throws DatahubClientException;

    public Deserializer<UpdateDataConnectorShardContextResult, UpdateDataConnectorShardContextRequest, Response> getUpdateConnectorShardContextResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, GetMeteringInfoRequest> getGetMeteringInfoRequestSer() throws DatahubClientException;

    public Deserializer<GetMeteringInfoResult, GetMeteringInfoRequest, Response> getGetMeteringInfoResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, ListDataConnectorRequest> getListDataConnectorRequestSer() throws DatahubClientException;

    public Deserializer<ListDataConnectorResult, ListDataConnectorRequest, Response> getListDataConnectorResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, CreateDataConnectorRequest> getCreateDataConnectorRequestSer() throws DatahubClientException;

    public Deserializer<CreateDataConnectorResult, CreateDataConnectorRequest, Response> getCreateDataConnectorResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, GetDataConnectorRequest> getGetDataConnectorRequestSer() throws DatahubClientException;

    public Deserializer<GetDataConnectorResult, GetDataConnectorRequest, Response> getGetDataConnectorResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, DeleteDataConnectorRequest> getDeleteDataConnectorRequestSer() throws DatahubClientException;

    public Deserializer<DeleteDataConnectorResult, DeleteDataConnectorRequest, Response> getDeleteDataConnectorResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, ReloadDataConnectorRequest> getReloadDataConnectorRequestSer() throws DatahubClientException;

    public Deserializer<ReloadDataConnectorResult, ReloadDataConnectorRequest, Response> getReloadDataConnectorResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, GetDataConnectorShardStatusRequest> getGetDataConnectorShardStatusRequestSer() throws DatahubClientException;

    public Deserializer<GetDataConnectorShardStatusResult, GetDataConnectorShardStatusRequest, Response> getGetDataConnectorShardStatusResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, AppendDataConnectorFieldRequest> getAppendDataConnectorFieldRequestSer() throws DatahubClientException;

    public Deserializer<AppendDataConnectorFieldResult, AppendDataConnectorFieldRequest, Response> getAppendDataConnectorFieldResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, CreateSubscriptionRequest> getCreateSubscriptionRequestSer() throws DatahubClientException;

    public Deserializer<CreateSubscriptionResult, CreateSubscriptionRequest, Response> getCreateSubscriptionResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, DeleteSubscriptionRequest> getDeleteSubscriptionRequestSer() throws DatahubClientException;

    public Deserializer<DeleteSubscriptionResult, DeleteSubscriptionRequest, Response> getDeleteSubscriptionResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, GetOffsetRequest> getGetOffsetRequestSer() throws DatahubClientException;

    public Deserializer<GetOffsetResult, GetOffsetRequest, Response> getGetOffsetResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, GetSubscriptionRequest> getGetSubscriptionRequestSer() throws DatahubClientException;

    public Deserializer<GetSubscriptionResult, GetSubscriptionRequest, Response> getGetSubscriptionResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, QuerySubscriptionRequest> getQuerySubscriptionRequestSer() throws DatahubClientException;

    public Deserializer<QuerySubscriptionResult, QuerySubscriptionRequest, Response> getQuerySubscriptionResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, CommitOffsetRequest> getCommitOffsetRequestSer() throws DatahubClientException;

    public Deserializer<CommitOffsetResult, CommitOffsetRequest, Response> getCommitOffsetResultDeser() throws  DatahubClientException;

    public Serializer<DefaultRequest, UpdateSubscriptionRequest> getUpdateSubscriptionRequestSer() throws DatahubClientException;

    public Deserializer<UpdateSubscriptionResult, UpdateSubscriptionRequest, Response> getUpdateSubscriptionResultDerser() throws DatahubClientException;

    public Serializer<DefaultRequest, GetDataConnectorDoneTimeRequest> getGetDataConnectorDoneTimeRequestSer() throws DatahubClientException;

    public Deserializer<GetDataConnectorDoneTimeResult, GetDataConnectorDoneTimeRequest, Response> getGetDataConnectorDoneTimeResultDeser() throws DatahubClientException;

    public Serializer<DefaultRequest, UpdateSubscriptionStateRequest> getUpdateSubscriptionStateRequestSer() throws DatahubClientException;

    public Deserializer<UpdateSubscriptionResult, UpdateSubscriptionStateRequest, Response> getUpdateSubscriptionStateResultDerser() throws DatahubClientException;
    
    public Serializer<DefaultRequest, InitOffsetContextRequest> getInitOffsetContextRequestSer() throws DatahubClientException;

    public Deserializer<InitOffsetContextResult, InitOffsetContextRequest, Response> getInitOffsetContextResultDerser() throws DatahubClientException;
    
    public Serializer<DefaultRequest, ResetOffsetRequest> getResetOffsetRequestSer() throws DatahubClientException;

    public Deserializer<CommitOffsetResult, ResetOffsetRequest, Response> getResetOffsetResultDeser() throws  DatahubClientException;

}
