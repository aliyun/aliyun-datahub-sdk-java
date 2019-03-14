package com.aliyun.datahub.client.model;

public enum CursorType {
    /** Used to get the cursor of the oldest data */
    OLDEST,

    /** Used to get the cursor of the newest data */
    LATEST,

    /** Used to get the cursor by the specified timestamp (in milliseconds) */
    SYSTEM_TIME,

    /** Used to get the cursor by the specified sequence */
    SEQUENCE,
}
