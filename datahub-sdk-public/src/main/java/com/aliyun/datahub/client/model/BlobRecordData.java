package com.aliyun.datahub.client.model;

public class BlobRecordData extends RecordData {
    /**
     * Binary data used to store BLOB data.
     */
    private byte[] data;

    public BlobRecordData(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
