package com.redhat.service.bridge.manager.metrics;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.redhat.service.bridge.infra.models.dto.ManagedResourceStatus;
import com.redhat.service.bridge.manager.models.ManagedResource;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class MetricsServiceImpl implements MetricsService {

    /*
     * Constant for the metric tag for the resource instance we're operating on e.g. Processor/Bridge.
     */
    static final String INSTANCE_TAG = "instance";

    @Inject
    MeterRegistry meterRegistry;

    @ConfigProperty(name = "rhose.metrics-name.operation-total-count")
    String operationTotalCountMetricName;

    @ConfigProperty(name = "rhose.metrics-name.operation-success-total-count")
    String operationTotalSuccessCountMetricName;

    @ConfigProperty(name = "rhose.metrics-name.operation-duration-seconds")
    String operatonDurationMetricName;

    @Override
    public <T extends ManagedResource> void onOperationStart(T managedResource, MetricsOperation operation) {
        incrementCounter(operationTotalCountMetricName, buildTags(managedResource, operation));
    }

    private void incrementCounter(String counterName, List<Tag> tags) {
        meterRegistry.counter(counterName, tags).increment();
    }

    /*
        Determines if the ManagedResource has reached the final end status for the operation that has been
        requested and therefore should be included  in the timing and count metrics.
     */
    private boolean isOperationEndStateReached(ManagedResource managedResource, MetricsOperation operation) {

        if (ManagedResourceStatus.FAILED == managedResource.getStatus()) {
            return true;
        }

        if (MetricsOperation.PROVISION == operation) {
            return ManagedResourceStatus.READY == managedResource.getStatus();
        }

        return ManagedResourceStatus.DELETED == managedResource.getStatus();
    }

    private boolean wasOperationSuccessful(ManagedResource managedResource, MetricsOperation operation) {
        if (MetricsOperation.PROVISION == operation) {
            return ManagedResourceStatus.READY == managedResource.getStatus();
        }

        return ManagedResourceStatus.DELETED == managedResource.getStatus();
    }

    @Override
    public <T extends ManagedResource> void onOperationComplete(T managedResource, MetricsOperation operation) {

        boolean complete = isOperationEndStateReached(managedResource, operation);

        if (complete) {
            boolean success = wasOperationSuccessful(managedResource, operation);
            if (success) {
                incrementCounter(operationTotalSuccessCountMetricName, buildTags(managedResource, operation));
            }

            recordOperationDuration(managedResource, operation);
        }
    }

    private List<Tag> buildTags(ManagedResource managedResource, MetricsOperation operation) {
        Tag instanceTag = Tag.of(INSTANCE_TAG, managedResource.getClass().getSimpleName().toLowerCase());
        return List.of(instanceTag, operation.getMetricTag());
    }

    private void recordOperationDuration(ManagedResource managedResource, MetricsOperation operation) {

        Duration operationDuration;

        if (MetricsOperation.PROVISION == operation) {
            operationDuration = Duration.between(managedResource.getSubmittedAt(), ZonedDateTime.now());
        } else {
            operationDuration = Duration.between(managedResource.getDeletedAt(), ZonedDateTime.now());
        }

        meterRegistry.timer(operatonDurationMetricName, buildTags(managedResource, operation)).record(operationDuration);
    }
}
