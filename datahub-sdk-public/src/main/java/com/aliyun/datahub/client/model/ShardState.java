package com.aliyun.datahub.client.model;

public enum ShardState {
    OPENING,
    /**
     * Only in this state, you can put records into the shard or get records from the shard.
     */
    ACTIVE,

    /**
     * Shard will become this state if you split a shard.
     */
    CLOSED,
    CLOSING,
    INACTIVE
}
