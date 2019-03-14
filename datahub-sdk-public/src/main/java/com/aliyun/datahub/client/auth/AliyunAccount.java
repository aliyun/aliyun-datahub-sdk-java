package com.aliyun.datahub.client.auth;

import com.aliyun.datahub.client.common.DatahubConstant;
import com.aliyun.datahub.client.http.HttpRequest;
import org.apache.commons.lang3.StringUtils;

import javax.ws.rs.core.HttpHeaders;
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
    public void addAuthHeaders(HttpRequest request) {
        if (securityToken != null) {
            request.header(DatahubConstant.X_DATAHUB_SECURITY_TOKEN, securityToken);
        }

        // Authorization must be last
        if (isNeedAuth()) {
            request.header(HttpHeaders.AUTHORIZATION, signer.genAuthSignature(request));
        }
    }

    @Override
    public String genAuthSignature(HttpRequest request) {
        return isNeedAuth() ? signer.genAuthSignature(request) : null;
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
