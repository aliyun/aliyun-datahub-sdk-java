package com.aliyun.datahub.client.common;

public abstract class ErrorCode {
    public static final String INVALID_PARAMETER = "InvalidParameter";
    public static final String INVALID_SUBSCRIPTION = "InvalidSubscription";
    public static final String INVALID_CURSOR = "InvalidCursor";
    /**
     * for later arrange error code
     */
    public static final String RESOURCE_NOT_FOUND = "ResourceNotFound";
    public static final String NO_SUCH_TOPIC = "NoSuchTopic";
    public static final String NO_SUCH_PROJECT = "NoSuchProject";
    public static final String NO_SUCH_SUBSCRIPTION = "NoSuchSubscription";
    public static final String NO_SUCH_SHARD = "NoSuchShard";
    public static final String NO_SUCH_CONNECTOR = "NoSuchConnector";
    public static final String NO_SUCH_METER_INFO = "NoSuchMeteringInfo";
    /**
     * for later arrange error code
     */
    public static final String SEEK_OUT_OF_RANGE = "SeekOutOfRange";
    public static final String RESOURCE_ALREADY_EXIST = "ResourceAlreadyExist";
    public static final String PROJECT_ALREADY_EXIST = "ProjectAlreadyExist";
    public static final String TOPIC_ALREADY_EXIST = "TopicAlreadyExist";
    public static final String CONNECTOR_ALREADY_EXIST = "ConnectorAlreadyExist";
    public static final String UN_AUTHORIZED = "Unauthorized";
    public static final String NO_PERMISSION = "NoPermission";
    public static final String INVALID_SHARD_OPERATION = "InvalidShardOperation";
    public static final String OPERATOR_DENIED = "OperationDenied";
    public static final String LIMIT_EXCEED = "LimitExceeded";
    public static final String ODPS_SERVICE_ERROR = "OdpsServiceError";
    public static final String MYSQL_SERVICE_ERROR = "MysqlServiceError";
    public static final String INTERNAL_SERVER_ERROR = "InternalServerError";
    public static final String SUBSCRIPTION_OFFLINE = "SubscriptionOffline";
    public static final String OFFSET_RESETED = "OffsetReseted";
    public static final String OFFSET_SESSION_CLOSED = "OffsetSessionClosed";
    public static final String OFFSET_SESSION_CHANGED = "OffsetSessionChanged";
    public static final String MALFORMED_RECORD = "MalformedRecord";
    public static final String NO_SUCH_CONSUMER = "NoSuchConsumer";
    public static final String CONSUMER_GROUP_IN_PROCESS = "ConsumerGroupInProcess";
}
