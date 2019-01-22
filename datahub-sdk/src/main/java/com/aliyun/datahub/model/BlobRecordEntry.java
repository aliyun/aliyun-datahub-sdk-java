package com.aliyun.datahub.model;

import com.aliyun.datahub.exception.DatahubClientException;
import org.apache.commons.codec.binary.Base64;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class BlobRecordEntry extends Record {
    private byte[] data;

    public BlobRecordEntry() {
        super();
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        if (data == null) {
            throw new DatahubClientException("record data is null");
        }
        this.data = data;
    }

    @Override
    public long getRecordSize() {
        return Base64.encodeBase64(data).length;
    }

    @Override
    public void clear() {
        data = null;
    }

    @Override
    public JsonNode toJsonNode() {

        if (data == null) {
            throw new DatahubClientException("record data is null");
        }

        ObjectNode node = super.toObjectNode();
        node.put("Data", data);
        return node;
    }
}
