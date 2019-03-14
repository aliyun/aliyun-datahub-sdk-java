package com.aliyun.datahub.client.e2e;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.e2e.common.Configure;
import com.aliyun.datahub.client.e2e.common.Constant;
import com.aliyun.datahub.client.model.*;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import static com.aliyun.datahub.client.e2e.common.Constant.*;

public abstract class BaseTest {
    public static boolean bCreateTopic = true;

    protected static final int SHARD_COUNT = 3;
    private static final int LIFE_CYCLE = 1;
    protected static final String TEST_PROJECT_NAME = Configure.getString(Constant.DATAHUB_PROJECT);

    protected static DatahubClient client;

    private static AtomicLong counter = new AtomicLong(0);

    protected String tupleTopicName;
    protected String blobTopicName;
    protected RecordSchema schema;

    @BeforeClass
    public static void setUpBeforeClass() {
        client = DatahubClientBuilder.newBuilder().setDatahubConfig(
                new DatahubConfig(
                        Configure.getString(DATAHUB_ENDPOINT),
                        new AliyunAccount(Configure.getString(DATAHUB_ACCESS_ID),
                                Configure.getString(DATAHUB_ACCESS_KEY)),
                        true
                )
        ).build();

        // clear exist project
        try {
            for (String topic : client.listTopic(TEST_PROJECT_NAME).getTopicNames()) {
                for (String connector : client.listConnector(TEST_PROJECT_NAME, topic).getConnectorNames()) {
                    client.deleteConnector(TEST_PROJECT_NAME, topic, ConnectorType.valueOf(connector.toUpperCase()));
                }
                client.deleteTopic(TEST_PROJECT_NAME, topic);
            }
        } catch (Exception e) {
            //
        }

    }

    @AfterClass
    public static void tearDownAfterClass() {

    }

    @BeforeMethod
    public void setUpBeforeMethod() {
        blobTopicName = getTestTopicName(RecordType.BLOB);
        tupleTopicName = getTestTopicName(RecordType.TUPLE);

        System.out.println("Start test on project:" + TEST_PROJECT_NAME + ", tuple topic:" + tupleTopicName + ", blob topic:" + blobTopicName);

        if (bCreateTopic) {
            schema = new RecordSchema() {{
                addField(new Field("a", FieldType.STRING));
                addField(new Field("b", FieldType.BIGINT));
                addField(new Field("c", FieldType.DOUBLE));
                addField(new Field("d", FieldType.TIMESTAMP));
                addField(new Field("e", FieldType.BOOLEAN));
                addField(new Field("f", FieldType.DECIMAL));
            }};

            client.createTopic(TEST_PROJECT_NAME, tupleTopicName, SHARD_COUNT, LIFE_CYCLE, RecordType.TUPLE, schema, "test tuple topic");
            client.createTopic(TEST_PROJECT_NAME, blobTopicName, SHARD_COUNT, LIFE_CYCLE, RecordType.BLOB, "test blob topic");

            client.waitForShardReady(TEST_PROJECT_NAME, tupleTopicName);
            client.waitForShardReady(TEST_PROJECT_NAME, blobTopicName);
        }
    }

    @AfterMethod
    public void tearDownAfterMethod() {
        if (bCreateTopic) {
            try {
                ListTopicResult listTopicResult = client.listTopic(TEST_PROJECT_NAME);
                for (String topic : listTopicResult.getTopicNames()) {
                    client.deleteTopic(TEST_PROJECT_NAME, topic);
                }
            } catch (Exception e) {
                //
            }
        }
    }


    protected static String getTestTopicName(RecordType type) {
        Date dNow = new Date();
        SimpleDateFormat ft = new SimpleDateFormat("yyyyMMddHHmmss");
        return String.format("%s_%s_%d", type.name().toLowerCase(), ft.format(dNow), counter.getAndIncrement());
    }

//    public static void main(String[] args) {
//        String projectName = "test_project";
//        DatahubClient client = DatahubClientBuilder.newBuilder().setDatahubConfig(
//                new DatahubConfig(
//                        Configure.getString(DATAHUB_ENDPOINT),
//                        new AliyunAccount(Configure.getString(DATAHUB_ACCESS_ID),
//                                Configure.getString(DATAHUB_ACCESS_KEY)),
//                        true
//                )
//        ).build();
//
//        // clear exist project
//        try {
//            for (String topic : client.listTopic(projectName).getTopicNames()) {
//                for (String connector : client.listConnector(projectName, topic).getConnectorNames()) {
//                    client.deleteConnector(projectName, topic, ConnectorType.valueOf(connector.toUpperCase()));
//                }
//                client.deleteTopic(projectName, topic);
//            }
//        } catch (Exception e) {
//            //
//        }
//    }
}
