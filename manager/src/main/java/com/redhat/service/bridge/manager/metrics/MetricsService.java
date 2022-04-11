package com.redhat.service.bridge.manager.metrics;

import com.redhat.service.bridge.manager.models.ManagedResource;

public interface MetricsService {

    <T extends ManagedResource> void onOperationStart(T managedResource, MetricsOperation operation);

    <T extends ManagedResource> void onOperationComplete(T managedResource, MetricsOperation operation);
}
