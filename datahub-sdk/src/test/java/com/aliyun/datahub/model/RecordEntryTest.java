package com.aliyun.datahub.model;

import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.util.DatahubTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.List;

@Test
public class RecordEntryTest {

    @Test
    public void testGetterSetter() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a");
        RecordEntry entry = new RecordEntry(schema);
        String shardId = DatahubTestUtils.getRandomString();
        String cursor = DatahubTestUtils.getRandomString();
        String nextCursor = DatahubTestUtils.getRandomString();
        long systemTime = DatahubTestUtils.getRandomTimestamp();
        entry.setShardId(shardId);
        entry.setSystemTime(systemTime);

        Assert.assertEquals(shardId, entry.getShardId());
        Assert.assertEquals(systemTime, entry.getSystemTime());
    }

    private void verifyRecordEntryByFields(List<Field> fileds, RecordEntry entry) {
        Assert.assertEquals(fileds.size(), entry.getFieldCount());
        Assert.assertEquals(fileds.size(), entry.getFields().length);
        for (int i = 0; i < fileds.size(); ++i) {
            Assert.assertEquals(fileds.get(i).getName(), entry.getFields()[i].getName());
            Assert.assertEquals(fileds.get(i).getType(), entry.getFields()[i].getType());
        }
    }

    @Test
    public void testConstructor() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a, bigint b, timestamp c, double d, boolean e, string f");
        RecordEntry entry = new RecordEntry(schema);
        verifyRecordEntryByFields(schema.getFields(), entry);

        Field [] fields = schema.getFields().toArray(new Field[schema.getFields().size()]);
        entry = new RecordEntry(fields);
        verifyRecordEntryByFields(schema.getFields(), entry);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testConstrutorWithInvalidValue1() {
        RecordSchema schema = null;
        RecordEntry entry = new RecordEntry(schema);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testConstructorWithInvalidValue2() {
        Field [] fields = null;
        RecordEntry entry = new RecordEntry(fields);
    }

    @Test
    public void testClearRecord() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a, bigint b, timestamp c, double d, boolean e, string f");
        RecordEntry entry = DatahubTestUtils.makeRecord(schema);
        Assert.assertNotNull(entry.getString("a"));
        Assert.assertNotNull(entry.getBigint("b"));
        Assert.assertNotNull(entry.getTimeStamp("c"));
        Assert.assertNotNull(entry.getDouble("d"));
        Assert.assertNotNull(entry.getBoolean("e"));
        Assert.assertNotNull(entry.getString("f"));

        entry.clear();

        Assert.assertNull(entry.getString("a"));
        Assert.assertNull(entry.getBigint("b"));
        Assert.assertNull(entry.getTimeStamp("c"));
        Assert.assertNull(entry.getDouble("d"));
        Assert.assertNull(entry.getBoolean("e"));
        Assert.assertNull(entry.getString("f"));
    }

    @Test
    public void testFieldIndex() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a, bigint b, timestamp c, double d, boolean e, string f");
        RecordEntry entry = new RecordEntry(schema);
        Assert.assertEquals(0, entry.getFieldIndex("a"));
        Assert.assertEquals(1, entry.getFieldIndex("b"));
        Assert.assertEquals(2, entry.getFieldIndex("c"));
        Assert.assertEquals(3, entry.getFieldIndex("d"));
        Assert.assertEquals(4, entry.getFieldIndex("e"));
        Assert.assertEquals(5, entry.getFieldIndex("f"));
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void TestInvalidFieldIndex() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a, bigint b, timestamp c, double d, boolean e, string f");
        RecordEntry entry = new RecordEntry(schema);
        entry.getFieldIndex("none-exist-field");
    }

    @Test
    public void testFieldValueByIndex() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a, bigint b, timestamp c, double d, boolean e");
        RecordEntry entry = new RecordEntry(schema);

        int stringIndex = 0;
        int numberIndex = 1;
        int timeIndex = 2;
        int decimalIndex = 3;
        int boolIndex = 4;

        String string = DatahubTestUtils.getRandomString();
        Long number = DatahubTestUtils.getRandomNumber();
        Long time = DatahubTestUtils.getRandomTimestamp();
        Double decimal = DatahubTestUtils.getRandomDecimal();
        Boolean bool = DatahubTestUtils.getRandomBoolean();

        entry.setString(stringIndex, string);
        entry.setBigint(numberIndex, number);
        entry.setTimeStamp(timeIndex, time);
        entry.setDouble(decimalIndex, decimal);
        entry.setBoolean(boolIndex, bool);

        Assert.assertEquals(string, entry.getString(stringIndex));
        Assert.assertEquals(number, entry.getBigint(numberIndex));
        Assert.assertEquals(time, entry.getTimeStamp(timeIndex));
        Assert.assertEquals(decimal, entry.getDouble(decimalIndex));
        Assert.assertEquals(bool, entry.getBoolean(boolIndex));
    }

    @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
    public void testSetValueWithInvalidIndex1() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a");
        RecordEntry entry = new RecordEntry(schema);
        entry.setString(-1, "a");
    }

    @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
    public void testSetValueWithInvalidIndex2() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a");
        RecordEntry entry = new RecordEntry(schema);
        entry.setString(5, "a");
    }

    @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
    public void testGetValueWithInvalidIndex1() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a");
        RecordEntry entry = new RecordEntry(schema);
        entry.setString(0, "a");
        entry.getString(-1);
    }

    @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
    public void testGetValueWithInvalidIndex2() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a");
        RecordEntry entry = new RecordEntry(schema);
        entry.setString(0, "a");
        entry.getString(5);
    }

    @Test
    public void testFieldValueByName() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a, bigint b, timestamp c, double d, boolean e");
        RecordEntry entry = new RecordEntry(schema);

        String stringName = "a";
        String numberName = "b";
        String timeName = "c";
        String decimalName = "d";
        String boolName = "e";

        String string = DatahubTestUtils.getRandomString();
        Long number = DatahubTestUtils.getRandomNumber();
        Long time = DatahubTestUtils.getRandomTimestamp();
        Double decimal = DatahubTestUtils.getRandomDecimal();
        Boolean bool = DatahubTestUtils.getRandomBoolean();

        entry.setString(stringName, string);
        entry.setBigint(numberName, number);
        entry.setTimeStamp(timeName, time);
        entry.setDouble(decimalName, decimal);
        entry.setBoolean(boolName, bool);

        Assert.assertEquals(string, entry.getString(stringName));
        Assert.assertEquals(number, entry.getBigint(numberName));
        Assert.assertEquals(time, entry.getTimeStamp(timeName));
        Assert.assertEquals(decimal, entry.getDouble(decimalName));
        Assert.assertEquals(bool, entry.getBoolean(boolName));
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testSetFieldValueByNoneExistName() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a");
        RecordEntry entry = new RecordEntry(schema);
        entry.setString("none-exist-name", "val");
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testGetFieldValueByNoneExistName() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a");
        RecordEntry entry = new RecordEntry(schema);
        entry.setString("a", "val");
        entry.getString("none-exist-name");
    }

    @Test
    public void testStringValue() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a");
        RecordEntry entry = new RecordEntry(schema);
        String emptyString = "";
        String nullString = null;
        entry.setString(0, emptyString);
        Assert.assertEquals(emptyString, entry.getString(0));
        emptyString = new String();
        entry.setString(0, emptyString);
        Assert.assertEquals(emptyString, entry.getString(0));
        entry.setString(0, nullString);
        Assert.assertEquals(nullString, entry.getString(0));
    }

    @Test
    public void testBooleanValue() {
        RecordSchema schema = DatahubTestUtils.createSchema("boolean a");
        RecordEntry entry = new RecordEntry(schema);
        Boolean trueValue = true;
        Boolean falseValue = false;
        Boolean nullValue = null;
        entry.setBoolean(0, trueValue);
        Assert.assertEquals(trueValue, entry.getBoolean(0));
        entry.setBoolean(0, falseValue);
        Assert.assertEquals(falseValue, entry.getBoolean(0));
        entry.setBoolean(0, nullValue);
        Assert.assertEquals(nullValue, entry.getBoolean(0));
    }

    @Test
    public void testBigintValue() {
        RecordSchema schema = DatahubTestUtils.createSchema("bigint a");
        RecordEntry entry = new RecordEntry(schema);
        Long zeroValue = 0L;
        Long maxValue = Long.MAX_VALUE;
        Long minValue = Long.MIN_VALUE + 1;
        Long nullValue = null;
        entry.setBigint(0, zeroValue);
        Assert.assertEquals(zeroValue, entry.getBigint(0));
        entry.setBigint(0, maxValue);
        Assert.assertEquals(maxValue, entry.getBigint(0));
        entry.setBigint(0, minValue);
        Assert.assertEquals(minValue, entry.getBigint(0));
        entry.setBigint(0, nullValue);
        Assert.assertEquals(nullValue, entry.getBigint(0));
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testInvalidBigintValue1() {
        RecordSchema schema = DatahubTestUtils.createSchema("bigint a");
        RecordEntry entry = new RecordEntry(schema);
        entry.setBigint(0, Long.MAX_VALUE + 1);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testInvalidBigintValue2() {
        RecordSchema schema = DatahubTestUtils.createSchema("bigint a");
        RecordEntry entry = new RecordEntry(schema);
        entry.setBigint(0, Long.MIN_VALUE);
    }

    @Test
    public void testTimeStampValue() {
        RecordSchema schema = DatahubTestUtils.createSchema("timestamp a");
        RecordEntry entry = new RecordEntry(schema);
        Long zeroValue = 0L;
        Long maxValue = Long.MAX_VALUE;
        Long minValue = Long.MIN_VALUE + 1;
        Long nullValue = null;
        entry.setTimeStamp(0, zeroValue);
        Assert.assertEquals(zeroValue, entry.getTimeStamp(0));
        entry.setTimeStamp(0, maxValue);
        Assert.assertEquals(maxValue, entry.getTimeStamp(0));
        entry.setTimeStamp(0, minValue);
        Assert.assertEquals(minValue, entry.getTimeStamp(0));
        entry.setTimeStamp(0, nullValue);
        Assert.assertEquals(nullValue, entry.getTimeStamp(0));
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testInvalidTimeStampValue1() {
        RecordSchema schema = DatahubTestUtils.createSchema("bigint a");
        RecordEntry entry = new RecordEntry(schema);
        entry.setTimeStamp(0, Long.MAX_VALUE + 1);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testInvalidTimeStampValue2() {
        RecordSchema schema = DatahubTestUtils.createSchema("bigint a");
        RecordEntry entry = new RecordEntry(schema);
        entry.setTimeStamp(0, Long.MIN_VALUE);
    }

    @Test
    public void testDoubleValue() {
        RecordSchema schema = DatahubTestUtils.createSchema("double a");
        RecordEntry entry = new RecordEntry(schema);
        Double zeroValue = 0.0;
        Double nullValue = null;
        entry.setDouble(0, zeroValue);
        Assert.assertEquals(zeroValue, entry.getDouble(0));
        entry.setDouble(0, nullValue);
        Assert.assertEquals(nullValue, entry.getDouble(0));
    }

    @Test
    public void testAttributes() {
        RecordSchema schema = DatahubTestUtils.createSchema("String a");
        RecordEntry entry = new RecordEntry(schema);

        String key = DatahubTestUtils.getRandomString();
        String val = DatahubTestUtils.getRandomString();
        entry.putAttribute(key, val);

        String newKey = "new" + key;
        String newVal = "val" + val;
        entry.putAttribute(newKey, newVal);

        Assert.assertEquals(val, entry.getAttributes().get(key));
        Assert.assertEquals(newVal, entry.getAttributes().get(newKey));

        String nullVal = null;
        entry.putAttribute(key, nullVal);

        Assert.assertEquals(nullVal, entry.getAttributes().get(key));

        String nullKey = null;
        entry.putAttribute(nullKey, val);

        Assert.assertEquals(val, entry.getAttributes().get(nullKey));

        entry.putAttribute(nullKey, nullVal);

        Assert.assertEquals(nullVal, entry.getAttributes().get(nullKey));

        entry.getAttributes().clear();
        Assert.assertNotEquals(entry.getAttributes().size(), 0);
    }

    @Test
    public void testTimestamp() {
        RecordSchema schema = DatahubTestUtils.createSchema("timestamp ms, timestamp us, timestamp date");
        RecordEntry entry = new RecordEntry(schema);

        long now = System.currentTimeMillis();
        Date d = new Date(now);
        long nowInUs = now * 1000;

        entry.setTimeStampInMs(0, now);
        entry.setTimeStampInUs(1, nowInUs);
        entry.setTimeStampInDate(2, d);

        Assert.assertEquals(entry.getTimeStampAsDate(0), d);
        Assert.assertEquals(entry.getTimeStampAsDate(1), d);
        Assert.assertEquals(entry.getTimeStampAsDate(2), d);
        Assert.assertEquals(entry.getTimeStampAsMs(0).longValue(), now);
        Assert.assertEquals(entry.getTimeStampAsMs(1).longValue(), now);
        Assert.assertEquals(entry.getTimeStampAsMs(2).longValue(), now);
        Assert.assertEquals(entry.getTimeStampAsUs(0).longValue(), nowInUs);
        Assert.assertEquals(entry.getTimeStampAsUs(1).longValue(), nowInUs);
        Assert.assertEquals(entry.getTimeStampAsUs(2).longValue(), nowInUs);

        entry = new RecordEntry(schema);
        entry.setTimeStampInMs(0, null);
        entry.setTimeStampInUs(1, null);
        entry.setTimeStampInDate(2, null);

        Assert.assertNull(entry.getTimeStampAsDate(0));
        Assert.assertNull(entry.getTimeStampAsDate(1));
        Assert.assertNull(entry.getTimeStampAsDate(2));
        Assert.assertNull(entry.getTimeStampAsMs(0));
        Assert.assertNull(entry.getTimeStampAsMs(1));
        Assert.assertNull(entry.getTimeStampAsMs(2));
        Assert.assertNull(entry.getTimeStampAsUs(0));
        Assert.assertNull(entry.getTimeStampAsUs(1));
        Assert.assertNull(entry.getTimeStampAsUs(2));

        entry = new RecordEntry(schema);
        entry.setTimeStampInMs("ms", now);
        entry.setTimeStampInUs("us", nowInUs);
        entry.setTimeStampInDate("date", d);

        Assert.assertEquals(entry.getTimeStampAsDate("ms"), d);
        Assert.assertEquals(entry.getTimeStampAsDate("us"), d);
        Assert.assertEquals(entry.getTimeStampAsDate("date"), d);
        Assert.assertEquals(entry.getTimeStampAsMs("ms").longValue(), now);
        Assert.assertEquals(entry.getTimeStampAsMs("us").longValue(), now);
        Assert.assertEquals(entry.getTimeStampAsMs("date").longValue(), now);
        Assert.assertEquals(entry.getTimeStampAsUs("ms").longValue(), nowInUs);
        Assert.assertEquals(entry.getTimeStampAsUs("us").longValue(), nowInUs);
        Assert.assertEquals(entry.getTimeStampAsUs("date").longValue(), nowInUs);

        entry = new RecordEntry(schema);
        entry.setTimeStampInMs("ms", null);
        entry.setTimeStampInUs("us", null);
        entry.setTimeStampInDate("date", null);

        Assert.assertNull(entry.getTimeStampAsDate("ms"));
        Assert.assertNull(entry.getTimeStampAsDate("us"));
        Assert.assertNull(entry.getTimeStampAsDate("date"));
        Assert.assertNull(entry.getTimeStampAsMs("ms"));
        Assert.assertNull(entry.getTimeStampAsMs("us"));
        Assert.assertNull(entry.getTimeStampAsMs("date"));
        Assert.assertNull(entry.getTimeStampAsUs("ms"));
        Assert.assertNull(entry.getTimeStampAsUs("us"));
        Assert.assertNull(entry.getTimeStampAsUs("date"));
    }

    @Test
    public void testGetRecordSize() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a, bigint b, timestamp c, double d, boolean e, string f");
        RecordEntry entry = new RecordEntry(schema);

        Assert.assertEquals(0, entry.getRecordSize());

        entry.setString(0, null);

        Assert.assertEquals(0, entry.getRecordSize());

        entry.setBigint(1, DatahubTestUtils.getRandomNumber());

        Assert.assertEquals(8, entry.getRecordSize());

        entry.setTimeStamp(2, DatahubTestUtils.getRandomTimestamp());

        Assert.assertEquals(8 + 8, entry.getRecordSize());

        entry.setDouble(3, DatahubTestUtils.getRandomDecimal());

        Assert.assertEquals(8 + 8 + 8, entry.getRecordSize());

        entry.setBoolean(4, DatahubTestUtils.getRandomBoolean());

        Assert.assertEquals(8 + 8 + 8 + 4, entry.getRecordSize());

        entry.setString(5, DatahubTestUtils.getRandomString(7));

        Assert.assertEquals(8 + 8 + 8 + 4 + 7, entry.getRecordSize());
    }

    @Test
    public void testToJsonNode() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a, bigint b, timestamp c, double d, boolean e");
        RecordEntry entry = new RecordEntry(schema);

        try {
            System.out.println(entry.toJsonNode().toString());
        } catch (DatahubClientException e) {
            Assert.assertEquals(e.getMessage(), "Parameter shardId/partitionKey/hashKey not set.");
        }

        entry.setShardId("shard-id");
        String jsonShardId = "{\"ShardId\":\"shard-id\",\"Attributes\":{},\"Data\":[null,null,null,null,null]}";

        Assert.assertEquals(jsonShardId, entry.toJsonNode().toString());

        entry.putAttribute("key", "val");
        String jsonAttribute = "{\"ShardId\":\"shard-id\",\"Attributes\":{\"key\":\"val\"},\"Data\":[null,null,null,null,null]}";

        Assert.assertEquals(jsonAttribute, entry.toJsonNode().toString());

        entry.setString("a", "string");
        String jsonString = "{\"ShardId\":\"shard-id\",\"Attributes\":{\"key\":\"val\"},\"Data\":[\"string\",null,null,null,null]}";

        Assert.assertEquals(jsonString, entry.toJsonNode().toString());

        entry.setBigint("b", 1L);
        String jsonBigint = "{\"ShardId\":\"shard-id\",\"Attributes\":{\"key\":\"val\"},\"Data\":[\"string\",\"1\",null,null,null]}";

        Assert.assertEquals(jsonBigint, entry.toJsonNode().toString());

        entry.setTimeStamp("c", 2L);
        String jsonTimeStamp = "{\"ShardId\":\"shard-id\",\"Attributes\":{\"key\":\"val\"},\"Data\":[\"string\",\"1\",\"2\",null,null]}";

        Assert.assertEquals(jsonTimeStamp, entry.toJsonNode().toString());

        entry.setDouble("d", 3.0);
        String jsonDouble = "{\"ShardId\":\"shard-id\",\"Attributes\":{\"key\":\"val\"},\"Data\":[\"string\",\"1\",\"2\",\"3.0\",null]}";

        Assert.assertEquals(jsonDouble, entry.toJsonNode().toString());

        entry.setBoolean("e", true);
        String jsonBoolean = "{\"ShardId\":\"shard-id\",\"Attributes\":{\"key\":\"val\"},\"Data\":[\"string\",\"1\",\"2\",\"3.0\",\"true\"]}";

        Assert.assertEquals(jsonBoolean, entry.toJsonNode().toString());
    }

    @Test
    public void testCaseInsensitive() {
        RecordSchema schema = DatahubTestUtils.createSchema("double ColDouble1, bigint ColBigint2, string ColString3, boolean ColBool4, timestamp ColTime5");
        RecordSchema lowerSchema = new RecordSchema();
        lowerSchema.addField(new Field("coldouble1", FieldType.DOUBLE));
        lowerSchema.addField(new Field("colbigint2", FieldType.BIGINT));
        lowerSchema.addField(new Field("colstring3", FieldType.STRING));
        lowerSchema.addField(new Field("colbool4", FieldType.BOOLEAN));
        lowerSchema.addField(new Field("coltime5", FieldType.TIMESTAMP));

        Assert.assertEquals(schema.toJsonString(), lowerSchema.toJsonString());

        RecordEntry entry = new RecordEntry(schema);
        entry.setDouble("COLDOUBLE1", 0.0);
        entry.setBigint("COLBIGINT2", 1L);
        entry.setString("COLSTRING3", "test");
        entry.setBoolean("COLBOOL4", true);
        entry.setTimeStamp("COLTIME5", 123456789000000L);

        Assert.assertEquals(entry.getDouble("ColDOUBLE1"), 0.0);
        Assert.assertEquals(entry.getBigint("ColBIGINT2").longValue(), 1L);
        Assert.assertEquals(entry.getString("ColSTRING3"), "test");
        Assert.assertTrue(entry.getBoolean("ColBOOL4"));
        Assert.assertEquals(entry.getTimeStamp("ColTIME5").longValue(), 123456789000000L);
    }
}
