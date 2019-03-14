package com.aliyun.datahub.oldsdk.model;

import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.model.PutRecordsRequest;
import com.aliyun.datahub.model.RecordEntry;
import com.aliyun.datahub.model.serialize.PutRecordsRequestJsonSer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigDecimal;

/**
 * Created by wz on 17/10/11.
 */
@Test
public class PutRecordsRequestTest {
    private RecordSchema schema;
    private String project;
    private String topic;

    @BeforeMethod
    public void init() {
        schema = new RecordSchema();
        schema.addField(new Field("a", FieldType.STRING));
        schema.addField(new Field("b", FieldType.BIGINT));
        schema.addField(new Field("c", FieldType.DOUBLE));
        schema.addField(new Field("d", FieldType.TIMESTAMP));
        schema.addField(new Field("e", FieldType.BOOLEAN));
        schema.addField(new Field("f", FieldType.DECIMAL));
        project = "test_project";
        topic = "test_topic";
    }

    private RecordEntry genRecord() {
        RecordEntry entry = new RecordEntry(schema);
        entry.setString("a", "abcdefghijklmnopqrstuvwxyz0123456789");
        entry.setBigint("b", 5L);
        entry.setDouble("c", 0.0);
        entry.setTimeStampInUs("d", 123456789000000L);
        entry.setBoolean("e", true);
        entry.setDecimal("f", new BigDecimal(10000.000001));
        entry.putAttribute("partition", "ds=2016");
        return entry;
    }

    @Test
    public void testAppendRecord() {
        PutRecordsRequest request = new PutRecordsRequest(project, topic);
        while (true) {
            if ( !request.appendRecord(genRecord())) {
                break;
            }
        }
        long approxSize = request.getApproxSize();
        PutRecordsRequestJsonSer Ser = PutRecordsRequestJsonSer.getInstance();
        DefaultRequest req = Ser.serialize(request);
        long actualSize = req.getBody().length;
        Assert.assertTrue(approxSize < request.getRequestLimitSize() * 0.99);
        Assert.assertTrue(approxSize > actualSize);
    }
}
