package com.aliyun.datahub.example;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.model.*;


public class Main {

    public static void main(String[] args) {
        String projectName = "";
        String topicName = "";
        String endpoint = "";
        String accessId = "";
        String accessKey = "";

        DatahubClient client = DatahubClientBuilder.newBuilder()
                .setDatahubConfig(new DatahubConfig(endpoint, new AliyunAccount(accessId, accessKey)))
                .setHttpConfig(new HttpConfig().setDebugRequest(true))
                .build();

        GetCursorResult getCursorResult = client.getCursor(projectName, topicName, "0", CursorType.OLDEST);

        GetRecordsResult getRecordsResult = client.getRecords(projectName, topicName, "0", getCursorResult.getCursor(), 10);
        for (RecordEntry entry : getRecordsResult.getRecords()) {
            BlobRecordData data = (BlobRecordData) entry.getRecordData();
            System.out.println(new String(data.getData()));
        }

    }



}
