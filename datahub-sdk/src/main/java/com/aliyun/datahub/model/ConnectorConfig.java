package com.aliyun.datahub.model;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.exception.DatahubClientException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Created by dongxiao.dx on 2017/4/5.
 */
public abstract class ConnectorConfig {
  public abstract ObjectNode toJsonNode();
  public abstract void ParseFromJsonNode(JsonNode node);
}
