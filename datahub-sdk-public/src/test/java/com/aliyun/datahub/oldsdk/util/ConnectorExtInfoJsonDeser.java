package com.aliyun.datahub.oldsdk.util;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.ConnectorConfig;
import com.aliyun.datahub.model.GetDataConnectorRequest;
import com.aliyun.datahub.model.serialize.Deserializer;
import com.aliyun.datahub.model.serialize.JsonErrorParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class ConnectorExtInfoJsonDeser implements Deserializer<ConnectorExtInfoJsonDeser.ConnectorExtInfoResult,GetDataConnectorRequest,Response> {
    public class ConnectorExtInfoResult extends ConnectorConfig {
        private String offsetStoreType;
        private String subscriptionId;

        public String getOffsetStoreType() {
            return offsetStoreType;
        }

        public String getSubscriptionId() {
            return subscriptionId;
        }

        public ObjectNode toJsonNode() {
            return null;
        }
        public void ParseFromJsonNode(JsonNode node) {
            if (node != null && !node.isNull()) {
                JsonNode sub = node.get("OffsetStoreType");
                if (sub != null && !sub.isNull()) {
                    offsetStoreType = sub.asText();
                }
                sub = node.get("SubscriptionId");
                if (sub != null && !sub.isNull()) {
                    subscriptionId = sub.asText();
                }
            } else {
                throw new DatahubClientException("Invalid response, missing config.");
            }
        }
    }
    @Override
    public ConnectorExtInfoResult deserialize(GetDataConnectorRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        ConnectorExtInfoResult rs = new ConnectorExtInfoResult();
        ByteArrayInputStream is = new ByteArrayInputStream(response.getBody());
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        JsonNode tree = null;
        try {
            tree = mapper.readTree(is);
        } catch (IOException e) {
            throw new DatahubServiceException(
                    "JsonParseError", "Parse body failed:" + response.getBody(), response);
        }

        JsonNode node = tree.get("Config");
        rs.ParseFromJsonNode(node);
        node = tree.get("ExtraInfo");
        rs.ParseFromJsonNode(node);
        return rs;
    }

    private ConnectorExtInfoJsonDeser() {
    }

    private static ConnectorExtInfoJsonDeser instance;

    public static ConnectorExtInfoJsonDeser getInstance() {
        if (instance == null)
            instance = new ConnectorExtInfoJsonDeser();
        return instance;
    }
}