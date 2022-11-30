package com.redhat.service.smartevents.shard.operator.v2;

import com.redhat.service.smartevents.infra.v2.api.models.dto.BridgeDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;

@ApplicationScoped
public class ManagedBridgeSyncService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedBridgeSyncService.class);

    @Inject
    ManagerClient managerClient;

    @Inject
    ManagedBridgeService managedBridgeService;

    public void syncManagedBridgeWithManager(){
        managerClient.fetchBridgesToDeployOrDelete()
                .onItem()
                .invoke(this::processDelta)
                .subscribe().with(
                        success -> LOGGER.debug("Successfully process ManagedBridge delta"),
                        failed -> LOGGER.debug("Fail to process ManagedBridge delta")
                );
    }

    private void processDelta(List<BridgeDTO> bridgeDTOList) {
        for (BridgeDTO bridgeDTO : bridgeDTOList) {
            if (bridgeDTO.getDeletionRequestedAt() != null) {
                managedBridgeService.deleteManagedBridge(bridgeDTO);
            } else {
                managedBridgeService.createManagedBridge(bridgeDTO);
            }
        }
    }
}
