package com.aliyun.datahub.model;

import java.util.Map;

public class InitOffsetContextResult {
    Map<String, OffsetContext> offsets;
    
    public Map<String, OffsetContext> getOffsets() {
        return offsets;
    }

    public void setOffsets(Map<String, OffsetContext> offsets) {
        this.offsets = offsets;
    }
}
