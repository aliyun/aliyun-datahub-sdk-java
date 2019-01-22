package com.aliyun.datahub.common.data;

import com.aliyun.datahub.util.DatahubTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

@Test
public class RecordSchemaTest {

    @Test
    public void testAddField() {
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("string", FieldType.STRING));
        schema.addField(new Field("bigint", FieldType.BIGINT));
        schema.addField(new Field("timestamp", FieldType.TIMESTAMP));
        schema.addField(new Field("boolean", FieldType.BOOLEAN));
        schema.addField(new Field("double", FieldType.DOUBLE));
        schema.addField(new Field("decimal", FieldType.DECIMAL));
    }

    @Test (expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Field is null.")
    public void testAddFieldWithNull() {
        RecordSchema schema = new RecordSchema();
        schema.addField(null);
    }

    @Test (expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "^Field.+duplicated\\.")
    public void testAddFieldWithDuplicateName() {
        RecordSchema schema = new RecordSchema();
        Field field = new Field("string", FieldType.STRING);
        schema.addField(field);
        schema.addField(field);
    }

    @Test
    public void testGetField() {
        RecordSchema schema = new RecordSchema();
        Field stringField = new Field("string", FieldType.STRING);
        Field bigintField = new Field("bigint", FieldType.BIGINT);
        Field timestampField = new Field("timestamp", FieldType.TIMESTAMP);
        Field booleanField = new Field("boolean", FieldType.BOOLEAN);
        Field doubleField = new Field("double", FieldType.DOUBLE);
        Field decimalField = new Field("decimal", FieldType.DECIMAL);
        schema.addField(stringField);
        schema.addField(bigintField);
        schema.addField(timestampField);
        schema.addField(booleanField);
        schema.addField(doubleField);
        schema.addField(decimalField);
        Assert.assertEquals(schema.getFields().size(), 6);
        Assert.assertEquals(schema.getField(0).getName(), stringField.getName());
        Assert.assertEquals(schema.getField(0).getType(), stringField.getType());
        Assert.assertEquals(schema.getField(1).getName(), bigintField.getName());
        Assert.assertEquals(schema.getField(1).getName(), bigintField.getName());
        Assert.assertEquals(schema.getField(2).getName(), timestampField.getName());
        Assert.assertEquals(schema.getField(2).getName(), timestampField.getName());
        Assert.assertEquals(schema.getField(3).getType(), booleanField.getType());
        Assert.assertEquals(schema.getField(3).getType(), booleanField.getType());
        Assert.assertEquals(schema.getField(4).getType(), doubleField.getType());
        Assert.assertEquals(schema.getField(4).getType(), doubleField.getType());
        Assert.assertEquals(schema.getField(5).getType(), decimalField.getType());
        Assert.assertEquals(schema.getField(5).getType(), decimalField.getType());

        Assert.assertEquals(schema.getField("string").getName(), stringField.getName());
        Assert.assertEquals(schema.getField("string").getType(), stringField.getType());
        Assert.assertEquals(schema.getField("bigint").getName(), bigintField.getName());
        Assert.assertEquals(schema.getField("bigint").getName(), bigintField.getName());
        Assert.assertEquals(schema.getField("timestamp").getName(), timestampField.getName());
        Assert.assertEquals(schema.getField("timestamp").getName(), timestampField.getName());
        Assert.assertEquals(schema.getField("boolean").getType(), booleanField.getType());
        Assert.assertEquals(schema.getField("boolean").getType(), booleanField.getType());
        Assert.assertEquals(schema.getField("double").getType(), doubleField.getType());
        Assert.assertEquals(schema.getField("double").getType(), doubleField.getType());
        Assert.assertEquals(schema.getField("decimal").getType(), decimalField.getType());
        Assert.assertEquals(schema.getField("decimal").getType(), decimalField.getType());
    }

    @Test (expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "idx out of range")
    public void testGetFieldWithInvalidIndex1() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a");
        schema.getField(-1);
    }

    @Test (expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "idx out of range")
    public void testGetFieldWithInvalidIndex2() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a");
        schema.getField(10);
    }

    @Test (expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "idx out of range")
    public void testGetFieldInEmptySchema() {
        RecordSchema schema = new RecordSchema();
        schema.getField(0);
    }

    @Test (expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "No such field:.*")
    public void testGetFieldByInvalidName() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a, string b");
        schema.getField("none-exist-field");
    }

    @Test (expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Field name is null")
    public void testGetFieldByNullName() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a, string b");
        schema.getField(null);
    }

    @Test (expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "No such field:.*")
    public void testGetFieldByNameInEmptySchema() {
        RecordSchema schema = new RecordSchema();
        schema.getField("field");
    }

    @Test
    public void testGetFieldIndex() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a, string b, string c, string d");

        Assert.assertEquals(schema.getFieldIndex("a"), 0);
        Assert.assertEquals(schema.getFieldIndex("b"), 1);
        Assert.assertEquals(schema.getFieldIndex("c"), 2);
        Assert.assertEquals(schema.getFieldIndex("d"), 3);
    }

    @Test (expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "No such field:.*")
    public void testGetFieldIndexWithInvalidName() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a, string b");
        schema.getFieldIndex("none-exist-field");
    }

    @Test (expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Field name is null")
    public void testGetFieldIndexWithNullName() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a, string b");
        schema.getFieldIndex(null);
    }

    @Test (expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "No such field:.*")
    public void testGetFieldIndexInEmptySchema() {
        RecordSchema schema = new RecordSchema();
        schema.getField("field");
    }

    @Test
    public void testSetFields() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a, bigint b, timestamp c, boolean d, double e");

        List<Field> fields = new ArrayList<Field>();
        Field stringField = new Field("string", FieldType.STRING);
        Field bigintField = new Field("bigint", FieldType.BIGINT);
        Field timestampField = new Field("timestamp", FieldType.TIMESTAMP);
        Field booleanField = new Field("boolean", FieldType.BOOLEAN);
        Field doubleField = new Field("double", FieldType.DOUBLE);
        Field decimalField = new Field("decimal", FieldType.DECIMAL);
        fields.add(stringField);
        fields.add(bigintField);
        fields.add(timestampField);
        fields.add(booleanField);
        fields.add(doubleField);
        fields.add(decimalField);
        schema.setFields(fields);

        Assert.assertEquals(schema.getFields().size(), 6);
        Assert.assertEquals(schema.getField(0).getName(), stringField.getName());
        Assert.assertEquals(schema.getField(0).getType(), stringField.getType());
        Assert.assertEquals(schema.getField(1).getName(), bigintField.getName());
        Assert.assertEquals(schema.getField(1).getName(), bigintField.getName());
        Assert.assertEquals(schema.getField(2).getName(), timestampField.getName());
        Assert.assertEquals(schema.getField(2).getName(), timestampField.getName());
        Assert.assertEquals(schema.getField(3).getType(), booleanField.getType());
        Assert.assertEquals(schema.getField(3).getType(), booleanField.getType());
        Assert.assertEquals(schema.getField(4).getType(), doubleField.getType());
        Assert.assertEquals(schema.getField(4).getType(), doubleField.getType());
        Assert.assertEquals(schema.getField(5).getType(), decimalField.getType());
        Assert.assertEquals(schema.getField(5).getType(), decimalField.getType());
        fields.clear();
        schema.setFields(fields);

        Assert.assertEquals(schema.getFields().size(), 0);
    }

    @Test (expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Field list is null")
    public void testSetFieldsWithNull() {
        RecordSchema schema = DatahubTestUtils.createSchema("string a, bigint b, timestamp c, boolean d, double e");
        schema.setFields(null);
    }

    @Test
    public void testGetFields() {
        RecordSchema schema = new RecordSchema();

        List<Field> fields = schema.getFields();
        Assert.assertEquals(fields.size(), 0);

        Field stringField = new Field("string", FieldType.STRING);
        Field bigintField = new Field("bigint", FieldType.BIGINT);
        Field timestampField = new Field("timestamp", FieldType.TIMESTAMP);
        Field booleanField = new Field("boolean", FieldType.BOOLEAN);
        Field doubleField = new Field("double", FieldType.DOUBLE);
        Field decimalField = new Field("decimal", FieldType.DECIMAL);
        schema.addField(stringField);
        schema.addField(bigintField);
        schema.addField(timestampField);
        schema.addField(booleanField);
        schema.addField(doubleField);
        schema.addField(decimalField);

        fields = schema.getFields();
        Assert.assertEquals(fields.size(), 6);
        Assert.assertEquals(fields.get(0).getName(), stringField.getName());
        Assert.assertEquals(fields.get(0).getType(), stringField.getType());
        Assert.assertEquals(fields.get(1).getName(), bigintField.getName());
        Assert.assertEquals(fields.get(1).getName(), bigintField.getName());
        Assert.assertEquals(fields.get(2).getName(), timestampField.getName());
        Assert.assertEquals(fields.get(2).getName(), timestampField.getName());
        Assert.assertEquals(fields.get(3).getType(), booleanField.getType());
        Assert.assertEquals(fields.get(3).getType(), booleanField.getType());
        Assert.assertEquals(fields.get(4).getType(), doubleField.getType());
        Assert.assertEquals(fields.get(4).getType(), doubleField.getType());
        Assert.assertEquals(fields.get(5).getType(), decimalField.getType());
        Assert.assertEquals(fields.get(5).getType(), decimalField.getType());

        fields.clear();
        Assert.assertNotEquals(schema.getFields().size(), 0);
    }

    @Test
    public void testContainsField() {
        RecordSchema schema = new RecordSchema();

        Assert.assertFalse(schema.containsField("field"));

        schema = DatahubTestUtils.createSchema("string a");

        Assert.assertTrue(schema.containsField("a"));
        Assert.assertFalse(schema.containsField("b"));
    }

    @Test
    public void testToJsonNode() {
        RecordSchema schema = new RecordSchema();
        Assert.assertEquals(schema.toJsonString(), "{}");
        schema.addField(new Field("str", FieldType.STRING));
        Assert.assertEquals(schema.toJsonString(), "{\"fields\":[{\"type\":\"STRING\",\"name\":\"str\"}]}");
        schema.addField(new Field("int", FieldType.BIGINT));
        Assert.assertEquals(schema.toJsonString(), "{\"fields\":[{\"type\":\"STRING\",\"name\":\"str\"},{\"type\":\"BIGINT\",\"name\":\"int\"}]}");
        schema.addField(new Field("time", FieldType.TIMESTAMP));
        Assert.assertEquals(schema.toJsonString(), "{\"fields\":[{\"type\":\"STRING\",\"name\":\"str\"},{\"type\":\"BIGINT\",\"name\":\"int\"},{\"type\":\"TIMESTAMP\",\"name\":\"time\"}]}");
        schema.addField(new Field("bool", FieldType.BOOLEAN));
        Assert.assertEquals(schema.toJsonString(), "{\"fields\":[{\"type\":\"STRING\",\"name\":\"str\"},{\"type\":\"BIGINT\",\"name\":\"int\"},{\"type\":\"TIMESTAMP\",\"name\":\"time\"},{\"type\":\"BOOLEAN\",\"name\":\"bool\"}]}");
        schema.addField(new Field("double", FieldType.DOUBLE, true));
        Assert.assertEquals(schema.toJsonString(), "{\"fields\":[{\"type\":\"STRING\",\"name\":\"str\"},{\"type\":\"BIGINT\",\"name\":\"int\"},{\"type\":\"TIMESTAMP\",\"name\":\"time\"},{\"type\":\"BOOLEAN\",\"name\":\"bool\"},{\"type\":\"DOUBLE\",\"name\":\"double\",\"notnull\":true}]}");
        schema.addField(new Field("decimal", FieldType.DECIMAL, true));
        Assert.assertEquals(schema.toJsonString(), "{\"fields\":[{\"type\":\"STRING\",\"name\":\"str\"}," +
            "{\"type\":\"BIGINT\",\"name\":\"int\"},{\"type\":\"TIMESTAMP\",\"name\":\"time\"}," +
            "{\"type\":\"BOOLEAN\",\"name\":\"bool\"}," +
            "{\"type\":\"DOUBLE\",\"name\":\"double\",\"notnull\":true}," +
            "{\"type\":\"DECIMAL\",\"name\":\"decimal\",\"notnull\":true}]}");
    }
}
