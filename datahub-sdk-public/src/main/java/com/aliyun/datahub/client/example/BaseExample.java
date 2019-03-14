package com.aliyun.datahub.client.example;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;

public abstract class BaseExample {
    static final String TEST_ENDPOINT = "**datahub endpoint**";
    static final String TEST_PROJECT = "** datahub project **";
    static final String TEST_TOPIC_TUPLE = "** datahub tuple topic **";
    static final String TEST_TOPIC_BLOB = "** datahub blob topic **";
    static final String TEST_AK = "** access id **";
    static final String TEST_SK = "** access key **";

    static final String ODPS_ENDPOINT = "** odps endpoint **";
    static final String ODPS_TUNNEL_ENDPOINT = "** tunnel endpoint **";
    static final String ODPS_PROJECT = "** odps project **";
    static final String ODPS_TABLE = "** odps table **";

    static final String ADS_HOST = "** ads hot**";
    static final int ADS_PORT = 9999;
    static final String ADS_USER = "** ads user **";
    static final String ADS_PASSWORD = "** ads password **";
    static final String ADS_DATABASE = "** ads database **";
    static final String ADS_TABLE = "** ads table **";

    protected boolean enablePb = false;
    protected DatahubClient client;

    public BaseExample() {
        createClient();
    }

    protected void createClient() {
        this.client = DatahubClientBuilder.newBuilder().setDatahubConfig(
                new DatahubConfig(TEST_ENDPOINT, new AliyunAccount(TEST_AK, TEST_SK), enablePb)
        ).build();
    }

    public abstract void runExample();
}
