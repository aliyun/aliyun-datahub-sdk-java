package com.aliyun.datahub.client.common;

public abstract class DatahubConstant {
    public static final String X_DATAHUB_CLIENT_VERSION = "x-datahub-client-version";
    public static final String X_DATAHUB_SECURITY_TOKEN = "x-datahub-security-token";
    public static final String X_DATAHUB_SOURCE_IP = "x-datahub-source-ip";
    public static final String X_DATAHUB_REQUEST_ID = "x-datahub-request-id";
    public static final String X_DATAHUB_REQUEST_ACTION = "x-datahub-request-action";
    public static final String X_DATAHUB_CONTENT_RAW_SIZE = "x-datahub-content-raw-size";
    public static final String PROP_INTER_HTTP_REQUEST = "prop.inter.HttpRequest";

    public static final String DATAHUB_CLIENT_VERSION_1 = "1.1";
    public static final String X_DATAHUB_PREFIX = "x-datahub-";
    public static final String DATAHUB_DATETIME_FORMAT = "EEE, dd MMM yyyy HH:mm:ss z";

    public static final String PROJECTS_URL = "/projects";
    public static final String PROJECT_URL  = "/projects/%s";
    public static final String TOPICS_URL   = "/projects/%s/topics";
    public static final String TOPIC_URL    = "/projects/%s/topics/%s";
    public static final String SHARDS_URL   = "/projects/%s/topics/%s/shards";
    public static final String SHARD_URL    = "/projects/%s/topics/%s/shards/%s";

    public static final String SUBSCRIPTIONS_URL = "/projects/%s/topics/%s/subscriptions";
    public static final String SUBSCRIPTION_URL = "/projects/%s/topics/%s/subscriptions/%s";
    public static final String SUBSCRIPTION_OFFSET_URL = "/projects/%s/topics/%s/subscriptions/%s/offsets";

    public static final String CONNECTORS_URL   = "/projects/%s/topics/%s/connectors";
    public static final String CONNECTOR_URL    = "/projects/%s/topics/%s/connectors/%s";

    public static final String ROUTER_URL = "/ops/router";
}
