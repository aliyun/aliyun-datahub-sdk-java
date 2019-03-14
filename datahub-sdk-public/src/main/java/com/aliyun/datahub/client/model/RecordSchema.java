package com.aliyun.datahub.client.model;

import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.impl.serializer.RecordSchemaDeserializer;
import com.aliyun.datahub.client.impl.serializer.RecordSchemaSerializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonSerialize(using = RecordSchemaSerializer.class)
@JsonDeserialize(using = RecordSchemaDeserializer.class)
public class RecordSchema {
    /** Schema contains a Field array, which you can treat as data Column */
    private List<Field> fieldList = new ArrayList<>();
    private Map<String, Field> fieldMap = new HashMap<>();

    public void addField(Field field) {
        if (field == null) {
            throw new InvalidParameterException("Field is null");
        }

        if (fieldMap.containsKey(field.getName())) {
            throw new InvalidParameterException("Filed already exists");
        }

        fieldMap.put(field.getName(), field);
        fieldList.add(field);
    }

    public Field getField(String name) {
        if (name == null) {
            throw new InvalidParameterException("Name is null");
        }

        return fieldMap.get(name.toLowerCase());
    }

    public Field getField(int idx) {
        if (idx < 0 || idx >= fieldList.size()) {
            throw new InvalidParameterException("idx out of range");
        }

        return fieldList.get(idx);
    }

    public int getFieldIndex(String name) {
        if (name == null) {
            throw new InvalidParameterException("Field name is null");
        }
        Field field = getField(name);
        return fieldList.indexOf(field);
    }

    public List<Field> getFields() {
        return fieldList;
    }

    public boolean containsField(String filedName) {
        return fieldMap.containsKey(filedName.toLowerCase());
    }
}
