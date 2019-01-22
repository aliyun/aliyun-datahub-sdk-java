package com.aliyun.datahub.model;

import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;

public class RecordEntry extends Record {
    private Field[] fields;
    private Object[] values;
    private HashMap<String, Integer> nameMap = new HashMap<String, Integer>();

    public RecordEntry(RecordSchema schema) {
        super();
        if (schema == null) {
            throw new IllegalArgumentException("schema must not be null");
        }
        init(schema.getFields().toArray(new Field[0]));
    }

    public RecordEntry(Field[] fields) {
        super();
        if (fields == null) {
            throw new IllegalArgumentException("fields list must not be null");
        }
        init(fields);
    }

    private void init(Field [] fields) {
        this.fields = fields;

        values = new Object[fields.length];

        for (int i = 0; i < fields.length; i++) {
            nameMap.put(fields[i].getName(), i);
        }
    }

    @Override
    public long getRecordSize() {
        long len = 0;
        for (int i = 0; i < values.length; ++i) {
            if (values[i] == null) {
                continue;
            }
            Field field = fields[i];
            if (field.getType() == FieldType.BIGINT) {
                len += 8;
            } else if (field.getType() == FieldType.BOOLEAN) {
                len += 4;
            } else if (field.getType() == FieldType.DOUBLE) {
                len += 8;
            } else if (field.getType() == FieldType.TIMESTAMP) {
                len += 8;
            } else if (field.getType() == FieldType.STRING) {
                len += ((String) values[i]).length();
            } else if (field.getType() == FieldType.DECIMAL) {
                len += ((BigDecimal) values[i]).toPlainString().length();
            } else {
                throw new IllegalArgumentException("unknown record type :" + field.getType().name());
            }
        }
        return len;
    }

    public int getFieldCount() {
        return values.length;
    }

    public Field[] getFields() {
        return fields;
    }

    public Object get(int idx) {
        return values[idx];
    }

    public Object get(String fieldName) {
        return values[getFieldIndex(fieldName)];
    }

    public void setBigint(int idx, Long value) {
        if (value != null && (value > Long.MAX_VALUE || value <= Long.MIN_VALUE)) {
            throw new IllegalArgumentException("InvalidData: Bigint out of range.");
        }
        values[idx] = value;
    }

    public Long getBigint(int idx) {
        return (Long) get(idx);
    }


    public void setBigint(String fieldName, Long value) {
        setBigint(getFieldIndex(fieldName), value);
    }


    public Long getBigint(String fieldName) {
        return (Long) get(fieldName);
    }


    public void setDouble(int idx, Double value) {
        values[idx] = value;
    }


    public Double getDouble(int idx) {
        return (Double) get(idx);
    }


    public void setDouble(String fieldName, Double value) {
        setDouble(getFieldIndex(fieldName), value);
    }

    public Double getDouble(String fieldName) {
        return (Double) get(fieldName);
    }

    public void setBoolean(int idx, Boolean value) {
        values[idx] = value;
    }


    public Boolean getBoolean(int idx) {
        return (Boolean) get(idx);
    }


    public void setBoolean(String fieldName, Boolean value) {
        setBoolean(getFieldIndex(fieldName), value);
    }

    public Boolean getBoolean(String fieldName) {
        return (Boolean) get(fieldName);
    }

    /**
     * set timestamp filed value by index, value should be microseconds
     * @param idx
     *     field index
     * @param microseconds
     *     the value of the field is microseconds
     */
    @Deprecated
    public void setTimeStamp(int idx, Long microseconds) {
        if (microseconds != null && (microseconds > Long.MAX_VALUE || microseconds <= Long.MIN_VALUE)) {
            throw new IllegalArgumentException("InvalidData: timestamp out of range.");
        }
        values[idx] = microseconds;
    }

    @Deprecated
    public Long getTimeStamp(int idx) {
        return (Long) get(idx);
    }

    /**
     * set timestamp filed value by index, value should be microseconds
     * @param idx
     *     field index
     * @param value
     *     the value of the field
     */
    public void setTimeStampInDate(int idx, Date value) {
        if (value != null) {
            setTimeStampInUs(idx, value.getTime() * 1000);
        } else {
            setTimeStampInUs(idx, null);
        }
    }

    public void setTimeStampInMs(int idx, Long milliseconds) {
        if (milliseconds != null) {
            setTimeStampInUs(idx, milliseconds * 1000);
        } else {
            setTimeStampInUs(idx, null);
        }
    }

    public void setTimeStampInUs(int idx, Long microseconds) {
        if (microseconds != null && (microseconds > Long.MAX_VALUE || microseconds <= Long.MIN_VALUE)) {
            throw new IllegalArgumentException("InvalidData: timestamp out of range.");
        }
        values[idx] = microseconds;
    }

    public Date getTimeStampAsDate(int idx) {
        Long t = getTimeStampAsUs(idx);
        return t == null ? null : new Date(t / 1000);
    }

    public Long getTimeStampAsMs(int idx) {
        Long t = getTimeStampAsUs(idx);
        return t == null ? null : t / 1000;
    }

    public Long getTimeStampAsUs(int idx) {
        return (Long) get(idx);
    }

    /**
     * set timestamp filed value by field name, value should be microseconds
     * @param fieldName
     *     field name
     * @param microseconds
     *     the value of the field is microseconds
     */
    @Deprecated
    public void setTimeStamp(String fieldName, Long microseconds) {
        setTimeStamp(getFieldIndex(fieldName), microseconds);
    }

    @Deprecated
    public Long getTimeStamp(String fieldName) {
        return (Long) get(fieldName);
    }

    /**
     * set timestamp filed value by field name, value should be microseconds
     * @param fieldName
     *     field name
     * @param value
     *     the value of the field
     */
    public void setTimeStampInDate(String fieldName, Date value) {
        setTimeStampInDate(getFieldIndex(fieldName), value);
    }

    public void setTimeStampInMs(String fieldName, Long milliseconds) {
        setTimeStampInMs(getFieldIndex(fieldName), milliseconds);
    }

    public void setTimeStampInUs(String fieldName, Long microseconds) {
        setTimeStampInUs(getFieldIndex(fieldName), microseconds);
    }

    public Date getTimeStampAsDate(String fieldName) {
        return getTimeStampAsDate(getFieldIndex(fieldName));
    }

    public Long getTimeStampAsMs(String fieldName) {
        return getTimeStampAsMs(getFieldIndex(fieldName));
    }

    public Long getTimeStampAsUs(String fieldName) {
        return getTimeStampAsUs(getFieldIndex(fieldName));
    }

    public void setString(int idx, String value) {
        values[idx] = value;
    }


    public String getString(int idx) {
        Object o = get(idx);
        if (o == null) {
            return null;
        }
        return (String) o;
    }

    public void setString(String fieldName, String value) {
        setString(getFieldIndex(fieldName), value);
    }

    public String getString(String fieldName) {
        return getString(getFieldIndex(fieldName));
    }

    public void setDecimal(int idx, BigDecimal value) {
        values[idx] = value;
    }


    public BigDecimal getDecimal(int idx) {
        Object o = get(idx);
        if (o == null) {
            return null;
        }
        return (BigDecimal) o;
    }

    public void setDecimal(String fieldName, BigDecimal value) {
        setDecimal(getFieldIndex(fieldName), value);
    }

    public BigDecimal getDecimal(String fieldName) {
        return getDecimal(getFieldIndex(fieldName));
    }

    public int getFieldIndex(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Field name is null");
        }
        Integer idx = nameMap.get(name.toLowerCase());
        if (idx == null) {
            throw new IllegalArgumentException("No such column:" + name.toLowerCase());
        }
        return idx;
    }

    public void clear() {
        for (int i = 0; i < values.length; i++) {
            values[i] = null;
        }
    }

    @Override
    public JsonNode toJsonNode() {
        ObjectNode node = super.toObjectNode();
        ArrayNode record = node.putArray("Data");
        for (int i = 0; i < this.getFieldCount(); i++) {
            if (this.get(i) != null) {
                if (fields[i].getType() == FieldType.DECIMAL) {
                    record.add(((BigDecimal)this.get(i)).toPlainString());
                } else {
                    record.add(String.valueOf(this.get(i)));
                }
            } else {
                record.add((JsonNode)null);
            }
        }
        return node;
    }
}
