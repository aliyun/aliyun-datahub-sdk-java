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

package com.aliyun.datahub.common.transport;

import java.io.IOException;

/**
 * Transport提供HTTP请求功能
 * 
 * 
 * Transport提供两种方式发起HTTP请求: 
 * 1. 通过 request()}发起的请求直接带请求的body数据，
 * 响应的{@code Response}对象包含响应的body数据
 * 
 * 2. 通过 connect()}发起的请求返回 Connection}对象, 
 * 请求和响应的body数据需要通过 Connection}上的输入和输出流处理
 * 
 */
public interface Transport {

    /**
     * 发起HTTP请求
     * 
     * 
     * 使用{@code request()}发起HTTP请求, 请求的body需要通过 DefaultRequest.setBody(byte[]
     * body)}提供, 响应的body数据直接通过返回的 Response.getBody()}获取
     * 
     *
     * @param req Request
     * @return Response
     * @throws IOException
     */
    public Response request(DefaultRequest req) throws IOException;

    /**
     * 发起HTTP请求
     *
     *
     * 使用{@code request()}发起HTTP请求, 请求的body需要通过 DefaultRequest.setBody(byte[]
     * body)}提供, 响应的body数据直接通过返回的 Response.getBody()}获取
     *
     *
     * @param req Request
     * @param endpoint
     * @return Response
     * @throws IOException
     */
    public Response request(DefaultRequest req, String endpoint) throws IOException;

    /**
     * 发起HTTP请求
     * 
     * 
     * 使用{@code connect()}发起请求, 请求的响应的body需要通过 Connection}来处理,
     * {@code Request} 对象上的body会被忽略, 返回的{@code Response}也获取不到body数据
     * 
     *
     * @param req
     * @return
     * @throws IOException
     */
    public Connection connect(DefaultRequest req) throws IOException;

    public void close();
}
