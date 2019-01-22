package com.aliyun.datahub.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class RetryUtil {
    private static final Logger LOG = LoggerFactory.getLogger(RetryUtil.class);

    private static final long MAX_SLEEP_MILLISECOND = 256 * 1000;

    public static <T> T executeWithRetry(Callable<T> callable, int retryTimes, long sleepTimeInMilliSecond,
                                         boolean exponential) throws Exception {
        Retry retry = new Retry();
        return retry.doRetry(callable, retryTimes, sleepTimeInMilliSecond, exponential);
    }

    private static class Retry {
        public <T> T doRetry(Callable<T> callable, int retryTimes, long sleepTimeInMilliSecond,
                             boolean exponential) throws Exception {
            if (null == callable) {
                throw new IllegalArgumentException("Callable!");
            }
            if (retryTimes < 0) {
                throw new IllegalArgumentException(String.format(
                        "retryTime[%d]!", retryTimes));
            }

            Exception saveException = null;
            for (int i = 0; i <= retryTimes; i++) {
                try {
                    return call(callable);
                } catch (Exception e) {
                    LOG.error("Exception when calling callable, " + (i + 1) + "ErrMsg:" + e.getMessage());
                    saveException = e;
                    if (i + 1 < retryTimes && sleepTimeInMilliSecond > 0) {
                        long timeToSleep;
                        if (exponential) {
                            timeToSleep = sleepTimeInMilliSecond * (long) Math.pow(2, i);
                            if (timeToSleep >= MAX_SLEEP_MILLISECOND) {
                                timeToSleep = MAX_SLEEP_MILLISECOND;
                            }
                        } else {
                            timeToSleep = sleepTimeInMilliSecond;
                            if (timeToSleep >= MAX_SLEEP_MILLISECOND) {
                                timeToSleep = MAX_SLEEP_MILLISECOND;
                            }
                        }

                        try {
                            Thread.sleep(timeToSleep);
                        } catch (InterruptedException ignored) {
                        }
                    }
                }
            }
            throw saveException;
        }

        protected <T> T call(Callable<T> callable) throws Exception {
            return callable.call();
        }
    }

}
