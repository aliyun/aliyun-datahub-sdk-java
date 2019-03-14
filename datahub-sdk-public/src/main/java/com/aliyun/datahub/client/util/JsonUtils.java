package com.aliyun.datahub.client.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class JsonUtils {
    private final static Logger LOGGER = LoggerFactory.getLogger(JsonUtils.class);
    private static ObjectMapper MAPPER;

    public static ObjectMapper getMAPPER() {
        return MAPPER;
    }

    static {
        initialize();
    }

    private static void initialize() {
        MAPPER = new ObjectMapper();
        MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public static <T> T fromJsonNode(JsonNode node, Class<T> cls) {
        try {
            return MAPPER.treeToValue(node, cls);
        } catch (IOException e) {
            LOGGER.warn("fromJsonNode fail. error:{}", e.getMessage());
        }
        return null;
    }

    public static JsonNode toJsonNode(String json) {
        if (StringUtils.isEmpty(json)) {
            return null;
        }

        try {
            return MAPPER.readTree(json);
        } catch (IOException e) {
            LOGGER.warn("toJsonNode fail. json:{}, error:{}", json, e.getMessage());
        }
        return null;
    }

    public static <T> T fromJson(String json, Class<T> cls) {
        if (StringUtils.isEmpty(json)) {
            return null;
        }

        try {
            return MAPPER.readValue(json, cls);
        } catch (IOException e) {
            LOGGER.warn("fromJson fail. json:{}, error:{}", json, e.getMessage());
        }
        return null;
    }

    public static <T> T fromJson(String json, TypeReference genericType) {
        if (StringUtils.isEmpty(json)) {
            return null;
        }

        try {
            return MAPPER.readValue(json, genericType);
        } catch (IOException e) {
            LOGGER.warn("fromJson fail. json:{}, error:{}", json, e.getMessage());
        }
        return null;
    }

    public static String toJson(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            LOGGER.warn("toJson fail. error:{}", e.getMessage());
        }
        return null;
    }
}
