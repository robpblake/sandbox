package com.redhat.service.smartevents.shard.operator.v2;

import com.redhat.service.smartevents.shard.operator.v2.comparators.Comparator;
import com.redhat.service.smartevents.shard.operator.v2.resources.ResourceDelta;
import io.fabric8.kubernetes.api.model.HasMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public interface DeltaProcessorService {

    Logger LOGGER = LoggerFactory.getLogger(DeltaProcessorService.class);

    default <T extends HasMetadata> boolean processDelta(Class<T> resourceType, Comparator<T> comparator, List<T> requestedResources, List<T> deployedResources) {
        ResourceDelta<T> resourceDelta = comparator.compare(requestedResources, deployedResources);
        if (!resourceDelta.HasChanged()) {
            LOGGER.debug("No delta found");
            return false;
        }
        resourceDelta.getCreated().forEach(resource -> onCreation(resourceType, resource));
        resourceDelta.getUpdated().forEach(resource -> onUpdation(resourceType, resource));
        resourceDelta.getDeleted().forEach(resource -> onDeletion(resourceType, resource));
        return true;
    }

    <T extends HasMetadata> void onCreation(Class<T> resourceType, T resource);
    <T extends HasMetadata> void onUpdation(Class<T> resourceType, T resource);
    <T extends HasMetadata> void onDeletion(Class<T> resourceType, T resource);
}
