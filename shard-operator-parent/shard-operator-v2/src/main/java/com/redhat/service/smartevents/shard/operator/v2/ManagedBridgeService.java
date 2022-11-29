package com.redhat.service.smartevents.shard.operator.v2;

import com.redhat.service.smartevents.shard.operator.v2.resources.ManagedBridge;

import java.util.List;

public interface ManagedBridgeService {

    List<ManagedBridge> fetchAllManagedBridges();
}
