package com.aliyun.datahub.client.model;

import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.exception.MalformedRecordException;
import com.aliyun.datahub.client.util.ValueCheckUtils;

import java.math.BigDecimal;
import java.util.List;

public class TupleRecordData extends RecordData {
    /**
     * Record schema of the TUPLE topic.
     */
    private RecordSchema recordSchema;

    /**
     * Object value which used to store TUPLE data.
     * The value has different type according to schema. For instance, BIGINT type stores as java.lang.Integer or java.lang.Long
     */
    private Object[] values;

    public TupleRecordData(RecordSchema recordSchema) {
        initData(recordSchema);
    }

    private void initData(RecordSchema schema) {
        this.recordSchema = schema;
        this.values = new Object[recordSchema.getFields().size()];
    }

    public void setField(String name, Object value) {
        if (!recordSchema.containsField(name)) {
            throw new InvalidParameterException("Filed not exist");
        }

        Field field = recordSchema.getField(name);
        if (!isFieldValid(field, value)) {
            throw new InvalidParameterException("Value is not consistent with schema, field type:" + field.getType());
        }
        int index = recordSchema.getFields().indexOf(field);
        values[index] = value;
    }

    public void setField(int index, Object value) {
        if (index >= recordSchema.getFields().size()) {
            throw new InvalidParameterException("Filed index out of range");
        }

        Field field = recordSchema.getFields().get(index);
        if (!isFieldValid(field, value)) {
            throw new InvalidParameterException("Value is not consistent with schema");
        }
        values[index] = value;
    }

    public Object getField(String name) {
        Field field = recordSchema.getField(name);
        int index = recordSchema.getFields().indexOf(field);
        if (index < values.length) {
            return values[index];
        }

        return null;
    }

    public Object getField(int idx) {
        Field field = recordSchema.getField(idx);
        int index = recordSchema.getFields().indexOf(field);
        if (index < values.length) {
            return values[index];
        }

        return null;
    }

    public RecordSchema getRecordSchema() {
        return recordSchema;
    }

    private boolean isFieldValid(Field field, Object value) {
        if (value == null) {
            if (field.isAllowNull()) {
                return true;
            }
            throw new InvalidParameterException("field: " + field.getName() + " not allow null");
        }

        switch (field.getType()) {
            case STRING:
                return ValueCheckUtils.checkString(value);
            case BIGINT:
                return ValueCheckUtils.checkBigint(value);
            case DOUBLE:
                return ValueCheckUtils.checkDouble(value);
            case BOOLEAN:
                return ValueCheckUtils.checkBoolean(value);
            case TIMESTAMP:
                return ValueCheckUtils.checkTimestamp(value);
            case DECIMAL:
                return ValueCheckUtils.checkDecimal(value);
            default:
                return false;
        }
    }

    // ************* internal use *************
    private int internalIndex = 0;
    private String[] internalAuxValues;
    public TupleRecordData(int valueSize) {
        internalAuxValues = new String[valueSize];
    }

    public void internalAddValue(String value) {
        internalAuxValues[internalIndex++] = value;
    }

    public void internalConvertAuxValues(RecordSchema schema) throws DatahubClientException {
        initData(schema);

        List<Field> fields = schema.getFields();
        for (int i = 0; i < fields.size() && i < internalAuxValues.length; ++i) {
            Field field = fields.get(i);
            String obj = internalAuxValues[i];

            if (obj == null) {
                if (field.isAllowNull()) {
                    continue;
                }
                throw new MalformedRecordException("Field: " + field.getName() + " not allow null");
            }

            try {
                switch (field.getType()) {
                    case BOOLEAN: {
                        if (!"true".equalsIgnoreCase(obj) && !"false".equalsIgnoreCase(obj)) {
                            throw new MalformedRecordException("Invalid boolean value: " + obj);
                        }
                        values[i] = Boolean.parseBoolean(obj);
                    }
                    break;
                    case DOUBLE:
                        values[i] = Double.parseDouble(obj);
                        break;
                    case STRING:
                        values[i] = obj;
                        break;
                    case BIGINT:
                    case TIMESTAMP:
                        values[i] = Long.parseLong(obj);
                        break;
                    case DECIMAL:
                        values[i] = new BigDecimal(obj);
                        break;
                }
            } catch (NumberFormatException e) {
                throw new MalformedRecordException("Invalid type cast. type: " + field.getType().name() + ", value:" + obj);
            }
        }

    }
    // ************* internal use *************
}
