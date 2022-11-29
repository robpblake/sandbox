package com.redhat.service.smartevents.manager.v2.api.user;

import com.redhat.service.smartevents.infra.v2.api.V2APIConstants;
import com.redhat.service.smartevents.infra.v2.api.models.dto.BridgeDTO;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

/**
 * This is a private API used by fleet shard operator to sync with the fleet manager so OpenAPI specs are hidden
 */
@Path(V2APIConstants.V2_SHARD_API_BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@RegisterRestClient
public class ShardBridgesSyncAPI {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShardBridgesSyncAPI.class);

    @GET
    public Response getBridges() {
        LOGGER.info("Shard asks for Bridges to deploy or delete");
        return Response.ok(getBridgeDTOs())
                .build();
    }

    List<BridgeDTO> getBridgeDTOs() {
        List<BridgeDTO> bridgeDTOList = new ArrayList<>();
        BridgeDTO bridgeDTO1 = new BridgeDTO();
        bridgeDTO1.setId("1");
        bridgeDTO1.setName("bridge1");
        bridgeDTO1.setCustomerId("cust1");
        bridgeDTOList.add(bridgeDTO1);

        BridgeDTO bridgeDTO2 = new BridgeDTO();
        bridgeDTO2.setId("2");
        bridgeDTO2.setName("bridge2");
        bridgeDTO2.setCustomerId("cust2");
        bridgeDTOList.add(bridgeDTO2);

        BridgeDTO bridgeDTO3 = new BridgeDTO();
        bridgeDTO3.setId("3");
        bridgeDTO3.setName("bridge3");
        bridgeDTO3.setCustomerId("cust3");
        bridgeDTOList.add(bridgeDTO3);

        return bridgeDTOList;
    }
}
