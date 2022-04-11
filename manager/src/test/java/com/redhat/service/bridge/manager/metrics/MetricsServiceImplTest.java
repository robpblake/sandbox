package com.redhat.service.bridge.manager.metrics;

import java.time.ZonedDateTime;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import com.redhat.service.bridge.infra.models.dto.ManagedResourceStatus;
import com.redhat.service.bridge.manager.models.Bridge;
import com.redhat.service.bridge.manager.models.ManagedResource;
import com.redhat.service.bridge.manager.models.Processor;
import com.redhat.service.bridge.manager.utils.Fixtures;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.quarkus.test.junit.QuarkusTest;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
public class MetricsServiceImplTest {

    @Inject
    MetricsService metricsService;

    @Inject
    MeterRegistry meterRegistry;

    @ConfigProperty(name = "rhose.metrics-name.operation-total-count")
    String operationTotalCountMetricName;

    @ConfigProperty(name = "rhose.metrics-name.operation-success-total-count")
    String operationTotalSuccessCountMetricName;

    @ConfigProperty(name = "rhose.metrics-name.operation-duration-seconds")
    String operatonDurationMetricName;

    private List<Tag> createdExpectedTags(ManagedResource managedResource, MetricsOperation operation) {
        return List.of(Tag.of(MetricsServiceImpl.INSTANCE_TAG, managedResource.getClass().getSimpleName().toLowerCase()), operation.getMetricTag());
    }

    @BeforeEach
    public void beforeEach() {
        meterRegistry.clear();
    }

    @Test
    public void onOperationStart_forProvisionOfBridge() {

        Bridge bridge = Fixtures.createBridge();
        metricsService.onOperationStart(bridge, MetricsOperation.PROVISION);

        List<Tag> expectedTags = createdExpectedTags(bridge, MetricsOperation.PROVISION);
        assertThat(meterRegistry.counter(operationTotalCountMetricName, expectedTags).count()).isEqualTo(1.0);
    }

    @Test
    public void onOperationStart_forProvisionOfProcessor() {

        Bridge bridge = Fixtures.createBridge();
        Processor processor = Fixtures.createProcessor(bridge, ManagedResourceStatus.ACCEPTED);

        metricsService.onOperationStart(processor, MetricsOperation.PROVISION);

        List<Tag> expectedTags = createdExpectedTags(processor, MetricsOperation.PROVISION);
        assertThat(meterRegistry.counter(operationTotalCountMetricName, expectedTags).count()).isEqualTo(1.0);
    }

    @Test
    public void onOperationComplete_forReadyBridge() {
        Bridge bridge = Fixtures.createBridge();
        bridge.setSubmittedAt(ZonedDateTime.now().minusMinutes(1));

        metricsService.onOperationComplete(bridge, MetricsOperation.PROVISION);

        List<Tag> expectedTags = createdExpectedTags(bridge, MetricsOperation.PROVISION);
        assertThat(meterRegistry.counter(operationTotalSuccessCountMetricName, expectedTags).count()).isEqualTo(1.0);
        assertThat(meterRegistry.timer(operatonDurationMetricName, expectedTags).totalTime(TimeUnit.MINUTES)).isNotEqualTo(0);
    }

    @Test
    public void onOperationComplete_forReadyProcessor() {
        Bridge bridge = Fixtures.createBridge();
        Processor processor = Fixtures.createProcessor(bridge, ManagedResourceStatus.READY);
        processor.setSubmittedAt(ZonedDateTime.now().minusMinutes(1));

        metricsService.onOperationComplete(processor, MetricsOperation.PROVISION);

        List<Tag> expectedTags = createdExpectedTags(processor, MetricsOperation.PROVISION);
        assertThat(meterRegistry.counter(operationTotalSuccessCountMetricName, expectedTags).count()).isEqualTo(1.0);
        assertThat(meterRegistry.timer(operatonDurationMetricName, expectedTags).totalTime(TimeUnit.MINUTES)).isNotEqualTo(0);
    }

    @Test
    public void onOperationComplete_forFailedBridge() {
        Bridge bridge = Fixtures.createBridge();
        bridge.setSubmittedAt(ZonedDateTime.now().minusMinutes(1));
        bridge.setStatus(ManagedResourceStatus.FAILED);

        metricsService.onOperationComplete(bridge, MetricsOperation.PROVISION);

        List<Tag> expectedTags = createdExpectedTags(bridge, MetricsOperation.PROVISION);
        assertThat(meterRegistry.counter(operationTotalSuccessCountMetricName, expectedTags).count()).isEqualTo(0);
        assertThat(meterRegistry.timer(operatonDurationMetricName, expectedTags).totalTime(TimeUnit.MINUTES)).isNotEqualTo(0);
    }

    @Test
    public void onOperationComplete_forIntermediateBridgeProvisioningOperation() {
        Bridge bridge = Fixtures.createBridge();
        bridge.setSubmittedAt(ZonedDateTime.now().minusMinutes(1));
        bridge.setStatus(ManagedResourceStatus.PROVISIONING);

        metricsService.onOperationComplete(bridge, MetricsOperation.PROVISION);

        List<Tag> expectedTags = createdExpectedTags(bridge, MetricsOperation.PROVISION);
        assertThat(meterRegistry.counter(operationTotalSuccessCountMetricName, expectedTags).count()).isEqualTo(0);
        assertThat(meterRegistry.timer(operatonDurationMetricName, expectedTags).totalTime(TimeUnit.MINUTES)).isEqualTo(0);
    }
}
