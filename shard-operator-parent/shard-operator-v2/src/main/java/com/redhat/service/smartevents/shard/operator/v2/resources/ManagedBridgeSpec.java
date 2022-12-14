package com.redhat.service.smartevents.shard.operator.v2.resources;

import java.util.Objects;

public class ManagedBridgeSpec {

    private String id;

    private String name;

    private String customerId;

    private KnativeBrokerConfigurationSpec knativeBrokerConfiguration = new KnativeBrokerConfigurationSpec();

    private DNSConfigurationSpec dnsConfiguration = new DNSConfigurationSpec();

    private String owner;

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public KnativeBrokerConfigurationSpec getKnativeBrokerConfiguration() {
        return knativeBrokerConfiguration;
    }

    public void setKnativeBrokerConfiguration(KnativeBrokerConfigurationSpec knativeBrokerConfiguration) {
        this.knativeBrokerConfiguration = knativeBrokerConfiguration;
    }

    public DNSConfigurationSpec getDnsConfiguration() {
        return dnsConfiguration;
    }

    public void setDnsConfiguration(DNSConfigurationSpec dnsConfiguration) {
        this.dnsConfiguration = dnsConfiguration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ManagedBridgeSpec that = (ManagedBridgeSpec) o;
        return Objects.equals(id, that.id) && Objects.equals(customerId, that.customerId) && Objects.equals(knativeBrokerConfiguration, that.knativeBrokerConfiguration)
                && Objects.equals(dnsConfiguration, that.dnsConfiguration) && Objects.equals(owner, that.owner);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, customerId, knativeBrokerConfiguration, dnsConfiguration, owner);
    }
}
