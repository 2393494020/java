package org.who.stream;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ProjectionMetrics {
    /**
     * 直方图
     * 统计某方法耗时，以纳秒为单位
     */
    private final Histogram latencyHist;

    ProjectionMetrics(MetricRegistry metricRegistry) {
        final Slf4jReporter reporter = Slf4jReporter.forRegistry(metricRegistry)
                .outputTo(log)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.SECONDS);
        latencyHist = metricRegistry.histogram(MetricRegistry.name(ProjectionMetrics.class, "latency"));
    }

    void latency(Duration duration) {
        latencyHist.update(duration.toMillis());
    }
}
