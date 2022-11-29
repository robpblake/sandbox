package com.redhat.service.smartevents.shard.operator.v2.converters;

import com.redhat.service.smartevents.infra.core.api.dto.KafkaConnectionDTO;
import com.redhat.service.smartevents.infra.core.exceptions.definitions.platform.InvalidURLException;
import com.redhat.service.smartevents.infra.v2.api.models.dto.BridgeDTO;
import com.redhat.service.smartevents.shard.operator.v2.providers.NamespaceProvider;
import com.redhat.service.smartevents.shard.operator.v2.resources.*;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.net.MalformedURLException;
import java.net.URL;

@ApplicationScoped
public class ManagedBridgeConverter {

    @Inject
    NamespaceProvider namespaceProvider;

    public ManagedBridge fromBridgeDTOToManageBridge(BridgeDTO bridgeDTO) {
        String namespace = namespaceProvider.getNamespaceName(bridgeDTO.getId());
        try {
            DNSConfigurationSpec dns = DNSConfigurationSpec.Builder.builder()
                    .host(new URL(bridgeDTO.getEndpoint()).getHost())
                    .tls(new TLSSpec(bridgeDTO.getTlsKey(), bridgeDTO.getTlsCertificate()))
                    .build();

            KafkaConnectionDTO kafkaConnectionDTO = bridgeDTO.getKafkaConnection();
            KafkaConfigurationSpec kafkaConfigurationSpec = KafkaConfigurationSpec.Builder.builder()
                    .bootstrapServers(kafkaConnectionDTO.getBootstrapServers())
                    .password(kafkaConnectionDTO.getClientSecret())
                    .user(kafkaConnectionDTO.getClientId())
                    .saslMechanism(kafkaConnectionDTO.getSaslMechanism())
                    .securityProtocol(kafkaConnectionDTO.getSecurityProtocol())
                    .topic(kafkaConnectionDTO.getTopic())
                    .build();

            return new ManagedBridge.Builder()
                    .withNamespace(namespace)
                    .withBridgeName(bridgeDTO.getName())
                    .withCustomerId(bridgeDTO.getCustomerId())
                    .withOwner(bridgeDTO.getOwner())
                    .withBridgeId(bridgeDTO.getId())
                    .withDnsConfigurationSpec(dns)
                    .withKnativeBrokerConfigurationSpec(new KNativeBrokerConfigurationSpec(kafkaConfigurationSpec))
                    .build();
        } catch (MalformedURLException e) {
            throw new InvalidURLException("Could not extract host from " + bridgeDTO.getEndpoint());
        }
    }
}
