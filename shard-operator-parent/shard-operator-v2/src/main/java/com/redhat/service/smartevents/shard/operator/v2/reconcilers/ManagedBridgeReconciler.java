package com.redhat.service.smartevents.shard.operator.v2.reconcilers;

import com.redhat.service.smartevents.shard.operator.v2.DeltaProcessorService;
import com.redhat.service.smartevents.shard.operator.v2.ManagedBridgeService;
import com.redhat.service.smartevents.shard.operator.v2.ManagerClient;
import com.redhat.service.smartevents.shard.operator.v2.comparators.Comparator;
import com.redhat.service.smartevents.shard.operator.v2.comparators.ManagedBridgeComparator;
import com.redhat.service.smartevents.shard.operator.v2.converters.ManagedBridgeConverter;
import com.redhat.service.smartevents.shard.operator.v2.resources.ManagedBridge;
import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.stream.Collectors;

@ApplicationScoped
public class ManagedBridgeReconciler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedBridgeReconciler.class);

    @Inject
    ManagerClient managerClient;

    @Inject
    ManagedBridgeService managedBridgeService;

    @Inject
    ManagedBridgeConverter managedBridgeConverter;

    @Inject
    @Named("managedBridgeDeltaProcessorService")
    DeltaProcessorService managedBridgeDeltaProcessorService;

    public void reconcile(){

        Uni<List<ManagedBridge>> requestedResources = createRequiredResources();

        Uni<List<ManagedBridge>> deployedResources = fetchDeployedResources();

        processDelta(requestedResources, deployedResources);
    }

    private Uni<List<ManagedBridge>> createRequiredResources() {
        return managerClient.fetchBridgesToDeployOrDelete().onItem().transform(bridges -> bridges.stream().map(bridgeDTO -> managedBridgeConverter.fromBridgeDTOToManageBridge(bridgeDTO)).collect(Collectors.toList()));
    }

    private Uni<List<ManagedBridge>> fetchDeployedResources() {
        return Uni.createFrom().item(managedBridgeService.fetchAllManagedBridges());
    }

    private void processDelta(Uni<List<ManagedBridge>> requestedResources, Uni<List<ManagedBridge>> deployedResources) {
        Uni.combine().all().unis(requestedResources, deployedResources).asTuple().onItem().invoke(tuple -> {
            Comparator<ManagedBridge> managedBridgeComparator = new ManagedBridgeComparator();
            boolean deltaProcessed = managedBridgeDeltaProcessorService.processDelta(ManagedBridge.class, managedBridgeComparator, tuple.getItem1(), tuple.getItem2());
            if (deltaProcessed) {
                LOGGER.info("Delta has been processed successfully");
            }
        }).subscribe().with(
                success -> LOGGER.info("Bridge reconcile success"),
                failure -> LOGGER.info("Bridge reconcile failed")
        );
    }
}
