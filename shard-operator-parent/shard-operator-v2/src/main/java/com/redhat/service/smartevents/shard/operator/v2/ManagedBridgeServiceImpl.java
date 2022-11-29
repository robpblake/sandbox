package com.redhat.service.smartevents.shard.operator.v2;

import com.redhat.service.smartevents.shard.operator.v2.resources.ManagedBridge;
import io.fabric8.kubernetes.client.KubernetesClient;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;

@ApplicationScoped
public class ManagedBridgeServiceImpl implements ManagedBridgeService {

    @Inject
    KubernetesClient kubernetesClient;

    @Override
    public List<ManagedBridge> fetchAllManagedBridges() {
        return kubernetesClient.resources(ManagedBridge.class).inAnyNamespace().list().getItems();
    }
}
