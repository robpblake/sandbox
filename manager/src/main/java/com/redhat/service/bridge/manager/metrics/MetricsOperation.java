package com.redhat.service.bridge.manager.metrics;

import io.micrometer.core.instrument.Tag;

/*
    Constant for the user operation we are capturing metrics for.
 */
public enum MetricsOperation {
    PROVISION("provision"),
    DELETE("delete");

    private final String tag;

    private MetricsOperation(String tag) {
        this.tag = tag;
    }

    Tag getMetricTag() {
        return Tag.of("operation", this.tag);
    }
}
