package com.aliyun.datahub.model;

import java.util.Map;

public class GetOffsetResult {
    Map<String, OffsetContext.Offset> offsets;
    Map<String, Long> versions;

    public Map<String, OffsetContext.Offset> getOffsets() {
        return offsets;
    }

    public void setOffsets(Map<String, OffsetContext.Offset> offsets) {
        this.offsets = offsets;
    }
    
    public Map<String, Long> getVersions() {
    	return versions;
    }
    
    public void setVersions(Map<String, Long> versions) {
    	this.versions = versions;
    }
}
