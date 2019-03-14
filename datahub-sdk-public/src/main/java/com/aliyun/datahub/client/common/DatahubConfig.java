package com.aliyun.datahub.client.common;

import com.aliyun.datahub.client.auth.Account;

public class DatahubConfig {
    private boolean enableBinary = false;
    private String endpoint;
    private Account account;

    public DatahubConfig(String endpoint, Account account) {
        this.endpoint = endpoint;
        this.account = account;
    }

    public DatahubConfig(String endpoint, Account account, boolean enableBinary) {
        this.endpoint = endpoint;
        this.account = account;
        this.enableBinary = enableBinary;
    }

    public boolean isEnableBinary() {
        return enableBinary;
    }

    public DatahubConfig setEnableBinary(boolean enableBinary) {
        this.enableBinary = enableBinary;
        return this;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public DatahubConfig setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public Account getAccount() {
        return account;
    }

    public DatahubConfig setAccount(Account account) {
        this.account = account;
        return this;
    }
}
