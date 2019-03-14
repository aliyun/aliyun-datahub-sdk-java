package com.aliyun.datahub.client.util;

import org.apache.commons.lang3.StringUtils;

public abstract class FormatUtils {
    public static boolean checkProjectName(String projectName, boolean create) {
        return isNameValid(projectName, 3, 32, create);
    }

    public static boolean checkProjectName(String projectName) {
        return checkProjectName(projectName, false);
    }

    public static boolean checkTopicName(String topicName, boolean create) {
        return isNameValid(topicName, 1, 128, create);
    }

    public static boolean checkTopicName(String topicName) {
        return checkTopicName(topicName, false);
    }

    public static boolean checkComment(String comment) {
        if (comment == null || /* (comment.length() < 1 ||*/ comment.length() > 1024) {
            return false;
        }
        return true;
    }

    public static boolean checkShardId(String shardId) {
        return StringUtils.isNumeric(shardId);
    }

    private static boolean isNameValid(String name, int minLen, int maxLen, boolean create) {
        if (name == null || (name.length() < minLen || name.length() > maxLen)) {
            return false;
        }

        for (int i = 0; i < name.length(); ++i) {
            char c = name.charAt(i);
            if (i == 0) {
                if (!Character.isAlphabetic(c) && create) {
                    return false;
                }
            } else {
                if (!Character.isAlphabetic(c) && !Character.isDigit(c) && c != '_') {
                    return false;
                }
            }
        }

        return true;
    }
}
