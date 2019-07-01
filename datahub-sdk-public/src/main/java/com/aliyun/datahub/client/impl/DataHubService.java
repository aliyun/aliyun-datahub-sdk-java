package com.aliyun.datahub.client.impl;

import com.aliyun.datahub.client.common.DatahubConstant;
import com.aliyun.datahub.client.impl.request.*;
import com.aliyun.datahub.client.impl.request.protobuf.GetRecordsRequestPB;
import com.aliyun.datahub.client.impl.request.protobuf.PutRecordsRequestPB;
import com.aliyun.datahub.client.model.*;
import com.aliyun.datahub.client.model.protobuf.GetRecordsResultPB;
import com.aliyun.datahub.client.model.protobuf.PutRecordsResultPB;
import retrofit2.Call;
import retrofit2.http.*;

public interface DataHubService {
    @GET("projects")
    Call<ListProjectResult> listProject();

    @GET("projects/{projectName}")
    Call<GetProjectResult> getProject(@Path("projectName") String projectName);

    @POST("projects/{projectName}")
    Call<CreateProjectResult> createProject(@Path("projectName") String projectName, @Body CreateProjectRequest request);

    @PUT("projects/{projectName}")
    Call<UpdateProjectResult> updateProject(@Path("projectName") String projectName, @Body UpdateProjectRequest request);

    @DELETE("projects/{projectName}")
    Call<DeleteProjectResult> deleteProject(@Path("projectName") String projectName);

    @POST("projects/{projectName}/topics/{topicName}")
    Call<CreateTopicResult> createTopic(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                        @Body CreateTopicRequest request);

    @PUT("projects/{projectName}/topics/{topicName}")
    Call<UpdateTopicResult> updateTopic(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                        @Body UpdateTopicRequest request);

    @GET("projects/{projectName}/topics/{topicName}")
    Call<GetTopicResult> getTopic(@Path("projectName") String projectName, @Path("topicName") String topicName);

    @DELETE("projects/{projectName}/topics/{topicName}")
    Call<DeleteTopicResult> deleteTopic(@Path("projectName") String projectName, @Path("topicName") String topicName);

    @GET("projects/{projectName}/topics")
    Call<ListTopicResult> listTopic(@Path("projectName") String projectName);

    @GET("projects/{projectName}/topics/{topicName}/shards")
    Call<ListShardResult> listShard(@Path("projectName") String projectName, @Path("topicName") String topicName);

    @POST("projects/{projectName}/topics/{topicName}/shards")
    Call<SplitShardResult> splitShard(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                      @Body SplitShardRequest request);

    @POST("projects/{projectName}/topics/{topicName}/shards")
    Call<MergeShardResult> mergeShard(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                      @Body MergeShardRequest request);

    @POST("projects/{projectName}/topics/{topicName}/shards/{shardId}")
    Call<GetCursorResult> getCursor(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                    @Path("shardId") String shardId,
                                    @Body GetCursorRequest request);

    @POST("projects/{projectName}/topics/{topicName}/shards")
    Call<PutRecordsResult> putRecords(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                      @Body PutRecordsRequest request);

    @POST("projects/{projectName}/topics/{topicName}/shards")
    @Headers(DatahubConstant.X_DATAHUB_REQUEST_ACTION + ":pub")
    Call<PutRecordsResultPB> putPbRecords(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                          @Body PutRecordsRequestPB request);

    @POST("projects/{projectName}/topics/{topicName}/shards/{shardId}")
    @Headers(DatahubConstant.X_DATAHUB_REQUEST_ACTION + ":pub")
    Call<PutRecordsByShardResult> putPbRecordsByShard(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                      @Path("shardId") String shardId, @Body PutRecordsRequestPB request);

    @POST("projects/{projectName}/topics/{topicName}/shards/{shardId}")
    Call<GetRecordsResult> getRecords(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                      @Path("shardId") String shardId, @Body GetRecordsRequest request);

    @POST("projects/{projectName}/topics/{topicName}/shards/{shardId}")
    @Headers(DatahubConstant.X_DATAHUB_REQUEST_ACTION + ":sub")
    Call<GetRecordsResultPB> getPBRecords(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                        @Path("shardId") String shardId, @Body GetRecordsRequestPB request);

    @POST("projects/{projectName}/topics/{topicName}")
    Call<AppendFieldResult> appendField(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                        @Body AppendFieldRequest request);

    @POST("projects/{projectName}/topics/{topicName}/shards/{shardId}")
    Call<GetMeterInfoResult> getMeterInfo(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                          @Path("shardId") String shardId,
                                          @Body GetMeterInfoRequest request);

    @GET("projects/{projectName}/topics/{topicName}/connectors")
    Call<ListConnectorResult> listConnector(@Path("projectName") String projectName, @Path("topicName") String topicName);

    @POST("projects/{projectName}/topics/{topicName}/connectors/{connectorType}")
    Call<CreateConnectorResult> createConnector(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                @Path("connectorType") String connectorType,
                                                @Body CreateConnectorRequest request);

    @GET("projects/{projectName}/topics/{topicName}/connectors/{connectorId}")
    Call<GetConnectorResult> getConnector(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                          @Path("connectorId") String connectorId);

    @POST("projects/{projectName}/topics/{topicName}/connectors/{connectorId}")
    Call<UpdateConnectorResult> updateConnector(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                @Path("connectorId") String connectorId, @Body UpdateConnectorRequest request);

    @DELETE("projects/{projectName}/topics/{topicName}/connectors/{connectorId}")
    Call<DeleteConnectorResult> deleteConnector(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                @Path("connectorId") String connectorId);

    @GET("projects/{projectName}/topics/{topicName}/connectors/{connectorId}?donetime")
    Call<GetConnectorDoneTimeResult> getConnectorDoneTime(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                          @Path("connectorId") String connectorId);

    @POST("projects/{projectName}/topics/{topicName}/connectors/{connectorId}")
    Call<ReloadConnectorResult> reloadConnector(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                @Path("connectorId") String connectorId, @Body ReloadConnectorRequest request);

    @POST("projects/{projectName}/topics/{topicName}/connectors/{connectorId}")
    Call<UpdateConnectorStateResult> updateConnectorState(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                          @Path("connectorId") String connectorId, @Body UpdateConnectorStateRequest request);

    @POST("projects/{projectName}/topics/{topicName}/connectors/{connectorId}")
    Call<UpdateConnectorOffsetResult> updateConnectorOffset(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                            @Path("connectorId") String connectorId, @Body UpdateConnectorOffsetRequest request);

    @POST("projects/{projectName}/topics/{topicName}/connectors/{connectorId}")
    Call<GetConnectorShardStatusResult> getConnectorShardStatus(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                                @Path("connectorId") String connectorId, @Body GetConnectorShardStatusRequest request);

    @POST("projects/{projectName}/topics/{topicName}/connectors/{connectorId}")
    Call<ConnectorShardStatusEntry> getConnectorShardStatusByShard(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                                   @Path("connectorId") String connectorId, @Body GetConnectorShardStatusRequest request);

    @POST("projects/{projectName}/topics/{topicName}/connectors/{connectorId}")
    Call<AppendConnectorFieldResult> appendConnectorField(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                          @Path("connectorId") String connectorId, @Body AppendConnectorFieldRequest request);

    @POST("projects/{projectName}/topics/{topicName}/subscriptions")
    Call<CreateSubscriptionResult> createSubscription(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                      @Body CreateSubscriptionRequest request);

    @GET("projects/{projectName}/topics/{topicName}/subscriptions/{subId}")
    Call<GetSubscriptionResult> getSubscription(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                   @Path("subId") String subId);

    @DELETE("projects/{projectName}/topics/{topicName}/subscriptions/{subId}")
    Call<DeleteSubscriptionResult> deleteSubscription(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                      @Path("subId") String subId);

    @POST("projects/{projectName}/topics/{topicName}/subscriptions")
    Call<ListSubscriptionResult> listSubscription(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                  @Body ListSubscriptionRequest request);

    @PUT("projects/{projectName}/topics/{topicName}/subscriptions/{subId}")
    Call<UpdateSubscriptionResult> updateSubscription(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                      @Path("subId") String subId, @Body UpdateSubscriptionRequest request);

    @PUT("projects/{projectName}/topics/{topicName}/subscriptions/{subId}")
    Call<UpdateSubscriptionStateResult> updateSubscriptionState(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                                @Path("subId") String subId, @Body UpdateSubscriptionRequest request);

    @POST("projects/{projectName}/topics/{topicName}/subscriptions/{subId}/offsets")
    Call<OpenSubscriptionSessionResult> openSubscriptionSession(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                                @Path("subId") String subId, @Body OpenSubscriptionSessionRequest request);

    @POST("projects/{projectName}/topics/{topicName}/subscriptions/{subId}/offsets")
    Call<GetSubscriptionOffsetResult> getSubscriptionOffset(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                            @Path("subId") String subId, @Body GetSubscriptionOffsetRequest request);

    @PUT("projects/{projectName}/topics/{topicName}/subscriptions/{subId}/offsets")
    Call<CommitSubscriptionOffsetResult> commitSubscriptionOffset(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                                  @Path("subId") String subId, @Body CommitSubscriptionOffsetRequest request);

    @PUT("projects/{projectName}/topics/{topicName}/subscriptions/{subId}/offsets")
    Call<ResetSubscriptionOffsetResult> resetSubscriptionOffset(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                                                @Path("subId") String subId, @Body ResetSubscriptionOffsetRequest request);

    @POST("projects/{projectName}/topics/{topicName}/subscriptions/{consumerGroup}")
    Call<HeartbeatResult> heartbeat(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                    @Path("consumerGroup") String consumerGroup, @Body HeartbeatRequest request);

    @POST("projects/{projectName}/topics/{topicName}/subscriptions/{consumerGroup}")
    Call<JoinGroupResult> joinGroup(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                    @Path("consumerGroup") String consumerGroup, @Body JoinGroupRequest request);

    @POST("projects/{projectName}/topics/{topicName}/subscriptions/{consumerGroup}")
    Call<SyncGroupResult> syncGroup(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                    @Path("consumerGroup") String consumerGroup, @Body SyncGroupRequest request);

    @POST("projects/{projectName}/topics/{topicName}/subscriptions/{consumerGroup}")
    Call<LeaveGroupResult> leaveGroup(@Path("projectName") String projectName, @Path("topicName") String topicName,
                                      @Path("consumerGroup") String consumerGroup, @Body LeaveGroupRequest request);
}
