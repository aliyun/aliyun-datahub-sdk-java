package com.aliyun.datahub.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Created by dongxiao.dx on 2017/4/5.
 */
public abstract class ConnectorConfig {
  public abstract ObjectNode toJsonNode();
  public abstract void ParseFromJsonNode(JsonNode node);
}
