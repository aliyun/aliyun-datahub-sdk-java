package com.aliyun.datahub.example;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.InvalidCursorException;
import com.aliyun.datahub.model.*;
import com.aliyun.datahub.wrapper.Topic;

import java.util.ArrayList;
import java.util.List;

public class DatahubWrapperExample {
    private String accessId = "";
    private String accessKey = "";
    private String endpoint = "";
    private DatahubClient client;
    private Topic topic;
    private String projectName = "project_test_example";
    private String topicName = "topic_test_example";

    public DatahubWrapperExample() {
        DatahubConfiguration conf = new DatahubConfiguration(new AliyunAccount(accessId, accessKey), endpoint);
        client = new DatahubClient(conf);
    }

    public void init() {

        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("a", FieldType.STRING));
        client.createTopic(projectName, topicName, 3, 3, RecordType.TUPLE, schema, "topic");
        Topic topic = Topic.Builder.build(projectName, topicName, client);
    }

    public void putRecords() {
        List<ShardEntry> shards = topic.listShard();
        RecordSchema schema = topic.getRecordSchema();
        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();

        int recordNum = 10;

        for (int n = 0; n < recordNum; n++) {
            //RecordData
            RecordEntry entry = new RecordEntry(schema);

            for (int i = 0; i < entry.getFieldCount(); i++) {
                entry.setString(i, String.valueOf(n));
            }

            //write record to different shard
            String shardId = shards.get(n % 3).getShardId();

            entry.setShardId(shardId);
            entry.putAttribute("partition", "ds=2016");

            recordEntries.add(entry);
        }

        int retryCount = 3;
        PutRecordsResult result = topic.putRecords(recordEntries, retryCount);

        if (result.getFailedRecordCount() > 0) {
            // handle failed records
            System.out.println("failed records:");
            for (RecordEntry record : result.getFailedRecords()) {
                System.out.println(record.toJsonNode().toString());
            }
        } else {
            // all records done
            System.out.println("successfully write all records");
        }
    }

    public void getRecords(String shardId) {
        // Get cursor by time
        // String cursor = topic.getCursor(shardId, System.currentTimeMillis() - 24 * 3600 * 1000 /* ms */);
        // Get Oldest cursor
        String cursor = topic.getCursor(shardId, GetCursorRequest.CursorType.OLDEST);
        RecordSchema schema = topic.getRecordSchema();

        // count must be greater then 0
        int count = 10;
        while (true) {
            try {
                GetRecordsResult recordRs = topic.getRecords(shardId, cursor, count);
                List<RecordEntry> recordEntries = recordRs.getRecords();

                for (RecordEntry entry : recordEntries) {
                    System.out.println(entry.toJsonNode().toString());
                    // processing record content here
                    for (int i = 0; i < schema.getFields().size(); i++) {
                        Field field = schema.getField(i);
                        switch (field.getType()) {
                            case BIGINT: {
                                long v = entry.getBigint(i);
                                break;
                            }
                            case STRING: {
                                String v = entry.getString(i);
                                break;
                            }
                            case BOOLEAN: {
                                boolean v = entry.getBoolean(i);
                                break;
                            }
                            case DOUBLE: {
                                double v = entry.getDouble(i);
                                break;
                            }
                            case TIMESTAMP: {
                                long v = entry.getTimeStamp(i);
                                break;
                            }
                            default:
                                break;
                        }

                    }

                }

                // no records available, just sleep for a while
                if (cursor.equals(recordRs.getNextCursor())) {
                    try {
                        Thread.sleep(10000);
                        System.out.println("No data available waiting...");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                // move cursor to next cursor
                cursor = recordRs.getNextCursor();
            } catch (InvalidCursorException ex) {
                cursor = topic.getCursor(shardId, GetCursorRequest.CursorType.OLDEST);
            }
        }
    }

    public static void main(String[] args) {
        DatahubWrapperExample example = new DatahubWrapperExample();
        try {
            example.init();
            example.putRecords();
            example.getRecords("0");
        } catch (DatahubClientException e) {
            e.printStackTrace();
        }
    }
}
