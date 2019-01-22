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
 * 阿里云认证账号
 * 
 * 
 * accessId/accessKey是阿里云用户的身份标识和认证密钥,请至http://www.aliyun.com查询。
 * 
 */
public class AliyunAccount implements Account {

    private String accessId;
    private String accessKey;
    private String securityToken;

    private AliyunRequestSigner signer;

    /**
     * 构造AliyunAccount对象
     *
     * @param accessId  AccessId
     * @param accessKey AccessKey
     */
    public AliyunAccount(String accessId, String accessKey) {
        this.accessId = accessId;
        this.accessKey = accessKey;

        signer = new AliyunRequestSigner(accessId, accessKey);
    }

    /**
     * AliyunAccount with sts token
     * @param accessId sts Access key id
     * @param accessKey Access key secret
     * @param securityToken sts security token
     */
    public AliyunAccount(String accessId, String accessKey, String securityToken) {
        this.accessId = accessId;
        this.accessKey = accessKey;
        this.securityToken = securityToken;

        signer = new AliyunRequestSigner(accessId, accessKey, securityToken);
    }

    /**
     * 获取当前帐号AccessID
     *
     * @return 当前帐号AccessID
     */
    public String getAccessId() {
        return accessId;
    }

    /**
     * 获取当前帐号AccessKey
     *
     * @return 当前帐号AccessKey
     */
    public String getAccessKey() {
        return accessKey;
    }

    /**
     * Get security token of this account
     * @return securityToken
     */
    public String getSecurityToken() {
        return securityToken;
    }

    /**
     * Set security token of account
     * @param securityToken
     */
    public void setSecurityToken(String securityToken) {
        this.securityToken = securityToken;
    }

    @Override
    public AccountProvider getType() {
        return AccountProvider.ALIYUN;
    }

    @Override
    public RequestSigner getRequestSigner() {
        return signer;
    }
}
