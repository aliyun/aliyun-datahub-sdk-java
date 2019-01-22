/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.datahub.common.data;

import com.aliyun.datahub.exception.DatahubClientException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RecordSchema {

    private ArrayList<Field> fields = new ArrayList<Field>();
    private HashMap<String, Integer> nameMap = new HashMap<String, Integer>();

    /**
     * 创建TopicSchema对象
     */
    public RecordSchema() {

    }

    /**
     * 表增加一列
     *
     * @param c 待新增的 Field}对象
     * @throws IllegalArgumentException c为空、列名已存在或不合法
     */
    public void addField(Field c) {

        if (c == null) {
            throw new IllegalArgumentException("Field is null.");
        }

        if (nameMap.containsKey(c.getName())) {
            throw new IllegalArgumentException("Field " + c.getName()
                    + " duplicated.");
        }

        nameMap.put(c.getName(), fields.size());

        fields.add(c);
    }

    /**
     * 获得列信息
     *
     * @param idx 列索引值
     * @return 列信息 Field}对象
     */
    public Field getField(int idx) {
        if (idx < 0 || idx >= fields.size()) {
            throw new IllegalArgumentException("idx out of range");
        }

        return fields.get(idx);
    }

    /**
     * 取得列索引
     *
     * @param name 列名
     * @return 列索引值
     * @throws IllegalArgumentException 列不存在
     */
    public int getFieldIndex(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Field name is null");
        }
        Integer idx = nameMap.get(name.toLowerCase());

        if (idx == null) {
            throw new IllegalArgumentException("No such field:" + name);
        }

        return idx;
    }

    /**
     * 取得列对象
     *
     * @param name 列名
     * @return  Field}对象
     * @throws IllegalArgumentException 列不存在
     */
    public Field getField(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Field name is null");
        }
        return fields.get(getFieldIndex(name.toLowerCase()));
    }

    public void setFields(List<Field> fields) {
        if (fields == null) {
            throw new IllegalArgumentException("Field list is null");
        }
        this.nameMap.clear();
        this.fields.clear();
        for (Field field : fields) {
            addField(field);
        }

    }

    /**
     * 获得列定义列表
     * 
     * 
     * 在返回的List上增加、删除元素导致TopicSchema增加或减少Field
     * 
     *
     * @return 被复制的 Field}列表
     */
    @SuppressWarnings("unchecked")
    public List<Field> getFields() {
        return (List<Field>) fields.clone();
    }

    /**
     * 判断是否包含对应列
     *
     * @param name 列名
     * @return 如果包含指定列，则返回true，否则返回false
     */
    public boolean containsField(String name) {
        return nameMap.containsKey(name.toLowerCase());
    }

    public String toJsonString() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode schema = mapper.createObjectNode();
        if (this.fields.size() > 0) {
            ArrayNode fields = schema.putArray("fields");
            for (Field f : this.fields) {
                ObjectNode field = fields.addObject();
                field.put("type", f.getType().toString());
                field.put("name", f.getName());
                if (f.getNotnull()) {
                    field.put("notnull", f.getNotnull());
                }
            }
        }
        try {
            return mapper.writeValueAsString(schema);
        } catch (IOException e) {
            throw new DatahubClientException("serialize error", e);
        }
    }
}