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

package com.aliyun.datahub.auth;

/**
 * @deprecated Please use classes in package 'com.aliyun.datahub.client'
 */
@Deprecated
public interface Account {

    /**
     * 淘宝账号,有两种认证方式（1）使用token认证（2）使用AccessId/AccessKey认证
     * 阿里云账号,使用AccessId/AccessKey认证
     */
    public enum AccountProvider {
        ALIYUN
    }

    /**
     * 获取当前账号的类型
     *
     * @return  AccountProvider}对象
     */
    public AccountProvider getType();

    /**
     * 获得用于对API请求进行签名的 RequestSigner}对象
     *
     * @return  RequestSigner}对象
     */
    public RequestSigner getRequestSigner();
}
