/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 *
 */
package com.aliyun.datahub;

/**
 * DatahubConstants.
 */

public interface DatahubConstants {

    public static String VERSION = "1.1";
    public static String LOCAL_ERROR_CODE = "Local Error";
    public static String ShardId = "ShardId";
    public static String State = "State";
    public static String ClosedTime = "ClosedTime";
    public static String BeginHashKey = "BeginHashKey";
    public static String EndHashKey = "EndHashKey";
    public static String LeftShardId = "LeftShardId";
    public static String RightShardId = "RightShardId";
    public static String ParentShardIds = "ParentShardIds";
    public static String PartitionKey = "PartitionKey";
    public static String HashKey = "HashKey";
    public static String FieldName = "FieldName";
    public static String CurrentSequence = "CurrentSequence";
    public static String StartSequence = "StartSequence";
    public static String EndSequence = "EndSequence";
    public static String MAX_SHARD_ID = String.valueOf(0xffffffffL);
    public static String ExtendInc = "INC";
    public static String ExtendTo = "TO";
}

