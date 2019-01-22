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

package com.aliyun.datahub.rest;

import com.aliyun.datahub.common.transport.Headers;

public class DatahubHttpHeaders implements Headers {
    public static final String HEADER_DATAHUB_REQUEST_ID = "x-datahub-request-id";
    public static final String HEADER_DATAHUB_CLIENT_VERSION = "x-datahub-client-version";
    public static final String HEADER_DATAHUB_CONTENT_RAW_SIZE = "x-datahub-content-raw-size";
    public static final String HEADER_DATAHUB_SOURCE_IP = "x-datahub-source-ip";
    public static final String HEADER_DATAHUB_SECURE_TRANSPORT = "x-datahub-secure-transport";
    public static final String HEADER_DATAHUB_SECURITY_TOKEN = "x-datahub-security-token";
}
