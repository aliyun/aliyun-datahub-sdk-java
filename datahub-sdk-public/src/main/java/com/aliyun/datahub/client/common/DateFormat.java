package com.aliyun.datahub.client.common;

import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

public abstract class DateFormat {
    public static SimpleDateFormat getDateTimeFormat() {
        SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DatahubConstant.DATAHUB_DATETIME_FORMAT, Locale.US);
        dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateTimeFormat;
    }
}
