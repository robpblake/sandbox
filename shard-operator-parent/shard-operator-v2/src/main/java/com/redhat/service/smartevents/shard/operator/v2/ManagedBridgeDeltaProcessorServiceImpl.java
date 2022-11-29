package com.redhat.service.smartevents.shard.operator.v2;

import com.redhat.service.smartevents.shard.operator.v2.providers.NamespaceProvider;
import com.redhat.service.smartevents.shard.operator.v2.resources.ManagedBridge;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;

@ApplicationScoped
@Named("managedBridgeDeltaProcessorService")
public class ManagedBridgeDeltaProcessorServiceImpl implements DeltaProcessorService {

    @Inject
    KubernetesClient kubernetesClient;

    @Inject
    NamespaceProvider namespaceProvider;

    @Override
    public <T extends HasMetadata> void onCreation(Class<T> resourceType, T resource) {
        if (resource instanceof ManagedBridge) {
            ManagedBridge managedBridge = (ManagedBridge) resource;
            namespaceProvider.fetchOrCreateNamespace(managedBridge);
            kubernetesClient.resources(resourceType).inNamespace(resource.getMetadata().getNamespace()).create(resource);
        }
    }

    @Override
    public <T extends HasMetadata> void onUpdation(Class<T> resourceType, T resource) {
        kubernetesClient.resources(resourceType).inNamespace(resource.getMetadata().getNamespace()).createOrReplace(resource);
    }

    @Override
    public <T extends HasMetadata> void onDeletion(Class<T> resourceType, T resource) {
        if (resource instanceof ManagedBridge) {
            ManagedBridge managedBridge = (ManagedBridge) resource;
            namespaceProvider.deleteNamespace(managedBridge);
        }
    }
}
