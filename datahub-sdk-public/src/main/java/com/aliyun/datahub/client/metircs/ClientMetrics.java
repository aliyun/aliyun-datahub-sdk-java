package com.aliyun.datahub.client.metircs;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class ClientMetrics {
    private static MetricProxy metricProxy = new MetricProxy();

    /**
     * Start metrics
     */
    public static void startMetrics() {
        startMetrics(true);
    }

    /**
     * Start metrics
     * @param enableLog Whether print metric information into logs.
     */
    public static void startMetrics(boolean enableLog) {
        metricProxy.start(enableLog);
    }

    public static Meter getMeter(MetricType type) {
        return metricProxy.getMeter(type);
    }

    public static Timer getTimer(MetricType type) {
        return metricProxy.getTimer(type);
    }

    private static class MetricProxy {
        private MetricRegistry metricRegistry;
        private Slf4jReporter slf4jReporter;

        public synchronized void start(boolean enableLog) {
            this.metricRegistry = new MetricRegistry();
            if (enableLog) {
                this.slf4jReporter = Slf4jReporter.forRegistry(metricRegistry)
                        .outputTo(LoggerFactory.getLogger("com.aliyun.datahub.client.metrics"))
                        .prefixedWith("Metric")
                        .convertDurationsTo(TimeUnit.MICROSECONDS)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .withLoggingLevel(Slf4jReporter.LoggingLevel.INFO)
                        .build();
                this.slf4jReporter.start(1, TimeUnit.MINUTES);
            }
        }

        public void stop() {
            if (slf4jReporter != null) {
                this.slf4jReporter.stop();
            }
        }

        public Meter getMeter(MetricType type) {
            if (metricRegistry == null) {
                return null;
            }

            String metricName = MetricRegistry.name(ClientMetrics.class, type.name(), "meter");
            Meter meter = findMeter(metricName);
            if (meter == null) {
                synchronized (ClientMetrics.class) {
                    meter = findMeter(metricName);
                    if (meter == null) {
                        meter = metricRegistry.meter(metricName);
                    }
                }
            }
            return meter;
        }

        public Timer getTimer(MetricType type) {
            if (metricRegistry == null) {
                return null;
            }

            String metricName = MetricRegistry.name(ClientMetrics.class, type.name(), "timer");
            Timer timer = findTimer(metricName);
            if (timer == null) {
                synchronized (ClientMetrics.class) {
                    timer = findTimer(metricName);
                    if (timer == null) {
                        timer = metricRegistry.timer(metricName);
                    }
                }
            }
            return timer;
        }

        private Timer findTimer(String timerName) {
            Map<String, Timer> timerMap = metricRegistry.getTimers();
            return timerMap == null ? null : timerMap.get(timerName);
        }

        private Meter findMeter(String meterName) {
            Map<String, Meter> meterMap = metricRegistry.getMeters();
            return meterMap == null ? null : meterMap.get(meterName);
        }
    }

    public enum MetricType {
        PUT_QPS,
        PUT_RPS,
        PUT_TPS,
        PUT_LATENCY,
        GET_QPS,
        GET_RPS,
        GET_TPS,
        GET_LATENCY,
    }
}
