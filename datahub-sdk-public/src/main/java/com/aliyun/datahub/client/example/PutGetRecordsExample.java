package com.aliyun.datahub.client.example;

import com.aliyun.datahub.client.model.*;

import java.util.ArrayList;
import java.util.List;

public class PutGetRecordsExample extends BaseExample {
    @Override
    public void runExample() {
        putRecordsIntoTopic("0");
        getRecordsFromTopic("0");

        putRecordsByShardIntoTopic("1");
        getRecordsFromTopic("1");
    }

    private void putRecordsIntoTopic(String shardId) {
        /* put records into BLOB topic */
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int cnt = 0; cnt < 10; ++cnt) {
            RecordEntry entry = new RecordEntry();
            entry.setShardId(shardId);
            entry.addAttribute("key1", "value1");
            entry.addAttribute("key2", "value2");
            entry.setRecordData(new BlobRecordData(("testdata-" + cnt).getBytes()));
            recordEntries.add(entry);
        }

        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT, TEST_TOPIC_BLOB, recordEntries);
        System.out.println("put failed record count: " + putRecordsResult.getFailedRecordCount());

        /* put records into TUPLE topic */
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("field1", FieldType.STRING));
        schema.addField(new Field("field2", FieldType.BIGINT));
        schema.addField(new Field("field3", FieldType.BOOLEAN));

        recordEntries = new ArrayList<>();
        for (int cnt = 0; cnt < 10; ++cnt) {
            RecordEntry entry = new RecordEntry();
            entry.setShardId(shardId);
            entry.addAttribute("key1", "value1");
            entry.addAttribute("key2", "value2");

            final int fcopy = cnt;
            TupleRecordData data = new TupleRecordData(schema) {{
                setField("field1", "test-data-" + fcopy);
                setField(1, fcopy);
                setField(2, false);
            }};
            entry.setRecordData(data);

            recordEntries.add(entry);
        }
        putRecordsResult = client.putRecords(TEST_PROJECT, TEST_TOPIC_TUPLE, recordEntries);
        System.out.println("put failed record count: " + putRecordsResult.getFailedRecordCount());
    }

    private void putRecordsByShardIntoTopic(String shardId) {
        enablePb = true;
        createClient();
        /* put records into BLOB topic */
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int cnt = 0; cnt < 10; ++cnt) {
            RecordEntry entry = new RecordEntry();
            entry.addAttribute("key1", "value1");
            entry.addAttribute("key2", "value2");
            entry.setRecordData(new BlobRecordData(("testdata-" + cnt).getBytes()));
            recordEntries.add(entry);
        }

        client.putRecordsByShard(TEST_PROJECT, TEST_TOPIC_BLOB, shardId, recordEntries);

        /* put records into TUPLE topic */
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("field1", FieldType.STRING));
        schema.addField(new Field("field2", FieldType.BIGINT));
        schema.addField(new Field("field3", FieldType.BOOLEAN));

        recordEntries = new ArrayList<>();
        for (int cnt = 0; cnt < 10; ++cnt) {
            RecordEntry entry = new RecordEntry();
            entry.addAttribute("key1", "value1");
            entry.addAttribute("key2", "value2");

            final int fcopy = cnt;
            TupleRecordData data = new TupleRecordData(schema) {{
                setField("field1", "test-data-" + fcopy);
                setField(1, fcopy);
                setField(2, false);
            }};
            entry.setRecordData(data);
            recordEntries.add(entry);
        }
        client.putRecordsByShard(TEST_PROJECT, TEST_TOPIC_TUPLE, shardId, recordEntries);
    }

    private void getRecordsFromTopic(String shardId) {
        /* get records from BLOB topic */
        GetCursorResult getCursorResult = client.getCursor(TEST_PROJECT, TEST_TOPIC_BLOB, shardId, CursorType.OLDEST);
        GetRecordsResult getRecordsResult = client.getRecords(TEST_PROJECT, TEST_TOPIC_BLOB, shardId, getCursorResult.getCursor(), 10);
        System.out.println("Read count:" + getRecordsResult.getRecordCount());

        /* get records from TUPLE topic */
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("field1", FieldType.STRING));
        schema.addField(new Field("field2", FieldType.BIGINT));
        schema.addField(new Field("field3", FieldType.STRING));
        getCursorResult = client.getCursor(TEST_PROJECT, TEST_TOPIC_TUPLE, shardId, CursorType.SEQUENCE, 0);
        getRecordsResult = client.getRecords(TEST_PROJECT, TEST_TOPIC_TUPLE, shardId, schema, getCursorResult.getCursor(), 100);
        for (RecordEntry entry : getRecordsResult.getRecords()) {
            TupleRecordData data = (TupleRecordData) entry.getRecordData();
            System.out.println("field1:" + data.getField(0) + ", field2:" + data.getField("field2") +
                    ", field3:" + data.getField(2));
        }
        System.out.println("Read count:" + getRecordsResult.getRecordCount());
    }
}
