package com.redhat.service.smartevents.manager;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.Transactional;

import com.redhat.service.smartevents.infra.models.VaultSecret;
import com.redhat.service.smartevents.manager.vault.VaultService;
import com.redhat.service.smartevents.processor.ResolvedGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.service.smartevents.infra.api.APIConstants;
import com.redhat.service.smartevents.infra.exceptions.definitions.user.AlreadyExistingItemException;
import com.redhat.service.smartevents.infra.exceptions.definitions.user.BadRequestException;
import com.redhat.service.smartevents.infra.exceptions.definitions.user.ItemNotFoundException;
import com.redhat.service.smartevents.infra.exceptions.definitions.user.ProcessorLifecycleException;
import com.redhat.service.smartevents.infra.models.ListResult;
import com.redhat.service.smartevents.infra.models.QueryInfo;
import com.redhat.service.smartevents.infra.models.dto.KafkaConnectionDTO;
import com.redhat.service.smartevents.infra.models.dto.ManagedResourceStatus;
import com.redhat.service.smartevents.infra.models.dto.ProcessorDTO;
import com.redhat.service.smartevents.infra.models.filters.BaseFilter;
import com.redhat.service.smartevents.infra.models.gateways.Action;
import com.redhat.service.smartevents.infra.models.gateways.Source;
import com.redhat.service.smartevents.infra.models.processors.ProcessorDefinition;
import com.redhat.service.smartevents.infra.models.processors.ProcessorType;
import com.redhat.service.smartevents.manager.api.models.requests.ProcessorRequest;
import com.redhat.service.smartevents.manager.api.models.responses.ProcessorResponse;
import com.redhat.service.smartevents.manager.connectors.ConnectorsService;
import com.redhat.service.smartevents.manager.dao.ProcessorDAO;
import com.redhat.service.smartevents.manager.models.Bridge;
import com.redhat.service.smartevents.manager.models.Processor;
import com.redhat.service.smartevents.manager.providers.InternalKafkaConfigurationProvider;
import com.redhat.service.smartevents.manager.providers.ResourceNamesProvider;
import com.redhat.service.smartevents.manager.workers.WorkManager;
import com.redhat.service.smartevents.processor.GatewayConfigurator;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

@ApplicationScoped
public class ProcessorServiceImpl implements ProcessorService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorServiceImpl.class);

    @Inject
    MeterRegistry meterRegistry;

    @Inject
    GatewayConfigurator gatewayConfigurator;
    @Inject
    InternalKafkaConfigurationProvider internalKafkaConfigurationProvider;
    @Inject
    ResourceNamesProvider resourceNamesProvider;

    @Inject
    ProcessorDAO processorDAO;

    @Inject
    BridgesService bridgesService;
    @Inject
    ConnectorsService connectorService;
    @Inject
    ShardService shardService;
    @Inject
    WorkManager workManager;

    @Inject
    VaultService vaultService;

    @Transactional
    @Override
    public Processor getProcessor(String bridgeId, String processorId, String customerId) {

        Bridge bridge = bridgesService.getBridge(bridgeId, customerId);
        Processor processor = processorDAO.findByIdBridgeIdAndCustomerId(bridge.getId(), processorId, bridge.getCustomerId());
        if (processor == null) {
            throw new ItemNotFoundException(String.format("Processor with id '%s' does not exist on Bridge '%s' for customer '%s'", processorId, bridgeId, customerId));
        }

        return processor;
    }

    @Override
    @Transactional
    public Processor createProcessor(String bridgeId, String customerId, ProcessorRequest processorRequest) {
        /* We cannot deploy Processors to a Bridge that is not Available */
        Bridge bridge = bridgesService.getReadyBridge(bridgeId, customerId);

        if (processorDAO.findByBridgeIdAndName(bridgeId, processorRequest.getName()) != null) {
            throw new AlreadyExistingItemException("Processor with name '" + processorRequest.getName() + "' already exists for bridge with id '" + bridgeId + "' for customer '" + customerId + "'");
        }

        ProcessorType processorType = processorRequest.getType();

        Processor newProcessor = new Processor();
        newProcessor.setType(processorType);
        newProcessor.setName(processorRequest.getName());
        newProcessor.setSubmittedAt(ZonedDateTime.now());
        newProcessor.setStatus(ManagedResourceStatus.ACCEPTED);
        newProcessor.setBridge(bridge);
        newProcessor.setShardId(shardService.getAssignedShardId(newProcessor.getId()));

        ProcessorDefinition definition = createProcessorDefinition(processorRequest, newProcessor);
        newProcessor.setDefinition(definition);

        // Processor, Connector and Work should always be created in the same transaction
        processorDAO.persist(newProcessor);
        connectorService.createConnectorEntity(newProcessor);
        workManager.schedule(newProcessor);

        LOGGER.info("Processor with id '{}' for customer '{}' on bridge '{}' has been marked for creation",
                    newProcessor.getId(),
                    newProcessor.getBridge().getCustomerId(),
                    newProcessor.getBridge().getId());

        return newProcessor;
    }

    private ProcessorDefinition createSourceDefinition(ProcessorRequest processorRequest, Processor processor) {
        Source source = processorRequest.getSource();
        Bridge bridge = processor.getBridge();

        ResolvedGateway<Source> resolvedGateway = gatewayConfigurator.getSourceResolver(source.getType())
                .resolve(source, bridge.getCustomerId(), bridge.getId(), processor.getId());

        ProcessorDefinition definition = new ProcessorDefinition(processorRequest.getFilters(), processorRequest.getTransformationTemplate(), resolvedGateway.getSanitizedRequest(), resolvedGateway.getSanitizedResolvedAction());
        if (resolvedGateway.containsSensitiveParameters()) {
            storeSensitiveParameters(processor, resolvedGateway);
        }

        return definition;
    }

    /*
        Stores the sensitive parameters as a secret an AWS Secrets Manager.
     */
    private void storeSensitiveParameters(Processor processor, ResolvedGateway resolvedGateway) {
        String secretName = resourceNamesProvider.getProcessorSecretName(processor.getId());
        processor.setVaultReference(secretName);

        VaultSecret eventBridgeSecret = new VaultSecret(secretName, resolvedGateway.getSensitiveParameters());
        //TODO - remote call within boundaries of DB TX. Re-work layout
        //TODO - parameterise wait time
        vaultService.createOrReplace(eventBridgeSecret).await().atMost(Duration.of(5, ChronoUnit.SECONDS));
    }

    private ProcessorDefinition createSinkDefinition(ProcessorRequest processorRequest, Processor processor) {
        Action action = processorRequest.getAction();
        Bridge bridge = processor.getBridge();

        ResolvedGateway<Action> resolvedGateway = gatewayConfigurator.getActionResolver(action.getType())
                .resolve(action, bridge.getCustomerId(), bridge.getId(), processor.getId());

        ProcessorDefinition definition = new ProcessorDefinition(processorRequest.getFilters(), processorRequest.getTransformationTemplate(), resolvedGateway.getSanitizedRequest(), resolvedGateway.getSanitizedResolvedAction());
        if (resolvedGateway.containsSensitiveParameters()) {
            storeSensitiveParameters(processor, resolvedGateway);
        }

        return definition;
    }

    private ProcessorDefinition createProcessorDefinition(ProcessorRequest request, Processor processor) {
        return processor.getType() == ProcessorType.SOURCE ? createSourceDefinition(request, processor) : createSinkDefinition(request, processor);
    }

    @Override
    @Transactional
    public Processor updateProcessor(String bridgeId, String processorId, String customerId, ProcessorRequest processorRequest) {
        // Attempt to load the Bridge. We cannot update a Processor if the Bridge is not available.
        bridgesService.getReadyBridge(bridgeId, customerId);

        // Extract existing definition
        Processor existingProcessor = getProcessor(bridgeId, processorId, customerId);
        if (!isProcessorActionable(existingProcessor)) {
            throw new ProcessorLifecycleException(String.format("Processor with id '%s' for customer '%s' is not in an actionable state.",
                                                                processorId,
                                                                customerId));
        }
        ProcessorDefinition existingDefinition = existingProcessor.getDefinition();
        Action existingAction = existingDefinition.getRequestedAction();
        Action existingResolvedAction = existingDefinition.getResolvedAction();
        Source existingSource = existingDefinition.getRequestedSource();

        // Validate update.
        // Name cannot be updated.
        if (!Objects.equals(existingProcessor.getName(), processorRequest.getName())) {
            throw new BadRequestException("It is not possible to update the Processor's name.");
        }
        if (!Objects.equals(existingProcessor.getType(), processorRequest.getType())) {
            throw new BadRequestException("It is not possible to update the Processor's Type.");
        }
        // See https://issues.redhat.com/browse/MGDOBR-516 for updating Action support
        if (!Objects.equals(existingAction, processorRequest.getAction())) {
            throw new BadRequestException("It is not possible to update the Processor's Action.");
        }
        if (!Objects.equals(existingSource, processorRequest.getSource())) {
            throw new BadRequestException("It is not possible to update the Processor's Source.");
        }

        // Construct updated definition
        Set<BaseFilter> updatedFilters = processorRequest.getFilters();
        String updatedTransformationTemplate = processorRequest.getTransformationTemplate();
        ProcessorDefinition updatedDefinition = existingProcessor.getType() == ProcessorType.SOURCE
                ? new ProcessorDefinition(updatedFilters, updatedTransformationTemplate, existingSource, existingResolvedAction)
                : new ProcessorDefinition(updatedFilters, updatedTransformationTemplate, existingAction, existingResolvedAction);

        // No need to update CRD if the definition is unchanged
        if (existingDefinition.equals(updatedDefinition)) {
            return existingProcessor;
        }

        // Create new definition copying existing properties
        existingProcessor.setModifiedAt(ZonedDateTime.now());
        existingProcessor.setStatus(ManagedResourceStatus.ACCEPTED);
        existingProcessor.setDefinition(updatedDefinition);

        // Processor and Work should always be created in the same transaction
        // Since updates to the Action are unsupported we do not need to update the Connector record.
        workManager.schedule(existingProcessor);

        LOGGER.info("Processor with id '{}' for customer '{}' on bridge '{}' has been marked for update",
                    existingProcessor.getId(),
                    existingProcessor.getBridge().getCustomerId(),
                    existingProcessor.getBridge().getId());

        return existingProcessor;
    }

    @Transactional
    @Override
    public List<Processor> findByShardIdWithReadyDependencies(String shardId) {
        return processorDAO.findByShardIdWithReadyDependencies(shardId);
    }

    @Transactional
    @Override
    public Processor updateProcessorStatus(ProcessorDTO processorDTO) {
        Bridge bridge = bridgesService.getBridge(processorDTO.getBridgeId());
        Processor p = processorDAO.findById(processorDTO.getId());
        if (p == null) {
            throw new ItemNotFoundException(String.format("Processor with id '%s' does not exist for Bridge '%s' for customer '%s'", bridge.getId(), bridge.getCustomerId(),
                                                          processorDTO.getCustomerId()));
        }
        p.setStatus(processorDTO.getStatus());
        p.setModifiedAt(ZonedDateTime.now());

        if (processorDTO.getStatus().equals(ManagedResourceStatus.DELETED)) {
            processorDAO.deleteById(processorDTO.getId());
        }
        if (processorDTO.getStatus().equals(ManagedResourceStatus.READY) && Objects.isNull(p.getPublishedAt())) {
            p.setPublishedAt(ZonedDateTime.now());
        }

        // Update metrics
        meterRegistry.counter("manager.processor.status.change",
                              Collections.singletonList(Tag.of("status", processorDTO.getStatus().toString()))).increment();

        return p;
    }

    @Transactional
    @Override
    public Long getProcessorsCount(String bridgeId, String customerId) {
        return processorDAO.countByBridgeIdAndCustomerId(bridgeId, customerId);
    }

    @Transactional
    @Override
    public ListResult<Processor> getProcessors(String bridgeId, String customerId, QueryInfo queryInfo) {
        Bridge bridge = bridgesService.getReadyBridge(bridgeId, customerId);
        return processorDAO.findByBridgeIdAndCustomerId(bridge.getId(), bridge.getCustomerId(), queryInfo);
    }

    @Override
    @Transactional
    public void deleteProcessor(String bridgeId, String processorId, String customerId) {
        Processor processor = processorDAO.findByIdBridgeIdAndCustomerId(bridgeId, processorId, customerId);
        if (processor == null) {
            throw new ItemNotFoundException(String.format("Processor with id '%s' does not exist on bridge '%s' for customer '%s'", processorId, bridgeId, customerId));
        }
        if (!isProcessorActionable(processor)) {
            throw new ProcessorLifecycleException("Processor could only be deleted if its in READY/FAILED state.");
        }

        // Processor and Connector deletion and related Work creation should always be in the same transaction
        processor.setStatus(ManagedResourceStatus.DEPROVISION);
        connectorService.deleteConnectorEntity(processor);
        workManager.schedule(processor);

        LOGGER.info("Processor with id '{}' for customer '{}' on bridge '{}' has been marked for deletion",
                    processor.getId(),
                    processor.getBridge().getCustomerId(),
                    processor.getBridge().getId());
    }

    private boolean isProcessorActionable(Processor processor) {
        // bridge could only be deleted if its in READY or FAILED state
        return processor.getStatus() == ManagedResourceStatus.READY || processor.getStatus() == ManagedResourceStatus.FAILED;
    }

    @Override
    public ProcessorDTO toDTO(Processor processor) {
        String topicName = processor.getType() == ProcessorType.SOURCE
                ? resourceNamesProvider.getProcessorTopicName(processor.getId())
                : resourceNamesProvider.getBridgeTopicName(processor.getBridge().getId());

        KafkaConnectionDTO kafkaConnectionDTO = new KafkaConnectionDTO(
                internalKafkaConfigurationProvider.getBootstrapServers(),
                internalKafkaConfigurationProvider.getClientId(),
                internalKafkaConfigurationProvider.getClientSecret(),
                internalKafkaConfigurationProvider.getSecurityProtocol(),
                topicName);
        ProcessorDTO dto = new ProcessorDTO();
        dto.setType(processor.getType());
        dto.setId(processor.getId());
        dto.setName(processor.getName());
        dto.setDefinition(processor.getDefinition());
        dto.setBridgeId(processor.getBridge().getId());
        dto.setCustomerId(processor.getBridge().getCustomerId());
        dto.setStatus(processor.getStatus());
        dto.setKafkaConnection(kafkaConnectionDTO);

        /*
            If the Processor has sensitive values, fetch them from the Vault and expose them to the
            Shard.
         */
        if (processor.getVaultReference() != null) {
            //TODO - parameterise timeout
            VaultSecret processorSecret = vaultService.get(processor.getVaultReference()).await().atMost(Duration.of(5, ChronoUnit.SECONDS));
            dto.getDefinition().getResolvedAction().getParameters().putAll(processorSecret.getValues());
        }

        return dto;
    }

    @Override
    public ProcessorResponse toResponse(Processor processor) {
        ProcessorResponse processorResponse = new ProcessorResponse();

        processorResponse.setType(processor.getType());
        processorResponse.setId(processor.getId());
        processorResponse.setName(processor.getName());
        processorResponse.setStatus(processor.getStatus());
        processorResponse.setPublishedAt(processor.getPublishedAt());
        processorResponse.setSubmittedAt(processor.getSubmittedAt());

        if (processor.getDefinition() != null) {
            ProcessorDefinition definition = processor.getDefinition();
            processorResponse.setFilters(definition.getFilters());
            processorResponse.setTransformationTemplate(definition.getTransformationTemplate());
            processorResponse.setAction(definition.getRequestedAction());
            processorResponse.setSource(definition.getRequestedSource());
        }

        if (processor.getBridge() != null) {
            processorResponse.setHref(APIConstants.USER_API_BASE_PATH + processor.getBridge().getId() + "/processors/" + processor.getId());
        }

        return processorResponse;
    }
}
