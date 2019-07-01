package com.aliyun.datahub.client.auth;

import com.aliyun.datahub.client.common.DatahubConstant;
import okhttp3.Request;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;

import java.util.Objects;

/**
 * Use AliyunAccount to provide essential user's information, such as accessId and accessKey.
 * <br>
 * You can get access information from 'http://www.aliyun.com'
 */
public class AliyunAccount implements Account {
    private String accessId;
    private String accessKey;
    private String securityToken;
    private AuthSigner signer;

    public AliyunAccount(String accessId, String accessKey) {
        this.accessId = accessId;
        this.accessKey = accessKey;
        this.signer = new AliyunAuthSigner(accessId, accessKey);
    }

    public AliyunAccount(String accessId, String accessKey, String securityToken) {
        this.accessId = accessId;
        this.accessKey = accessKey;
        this.securityToken = securityToken;
        this.signer = new AliyunAuthSigner(accessId, accessKey);
    }

    @Override
    public void addAuthHeaders(Request.Builder reqBuilder) {
        if (securityToken != null) {
            reqBuilder.addHeader(DatahubConstant.X_DATAHUB_SECURITY_TOKEN, securityToken);
        }

        // Authorization must be last
        if (isNeedAuth()) {
            Request copyRequest = reqBuilder.build();
            reqBuilder.addHeader(HttpHeaders.AUTHORIZATION, signer.genAuthSignature(copyRequest));
        }
    }

    private boolean isNeedAuth() {
        return !StringUtils.isEmpty(accessId) && !StringUtils.isEmpty(accessKey);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AliyunAccount that = (AliyunAccount) o;
        return Objects.equals(accessId, that.accessId) &&
                Objects.equals(accessKey, that.accessKey) &&
                Objects.equals(securityToken, that.securityToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessId, accessKey, securityToken);
    }
}
