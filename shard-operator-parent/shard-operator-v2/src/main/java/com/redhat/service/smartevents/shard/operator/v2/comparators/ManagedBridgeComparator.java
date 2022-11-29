package com.redhat.service.smartevents.shard.operator.v2.comparators;

import com.redhat.service.smartevents.shard.operator.v2.resources.ManagedBridge;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class ManagedBridgeComparator implements Comparator<ManagedBridge> {

    @Override
    public boolean compare(ManagedBridge requestedResource, ManagedBridge deployedResource) {
        return requestedResource.getSpec().equals(deployedResource.getSpec());
    }
}
