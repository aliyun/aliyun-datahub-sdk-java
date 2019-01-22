package com.aliyun.datahub.model;

public enum ConnectorShardStatus {
    CONTEXT_PLANNED,
    CONTEXT_EXECUTING,
    CONTEXT_HANG,
    CONTEXT_PAUSED,
    CONTEXT_FINISHED,
    CONTEXT_DELETED
}
