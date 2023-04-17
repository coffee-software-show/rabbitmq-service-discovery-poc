package com.example.rmqdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.function.client.ExchangeFilterFunctions;
import org.springframework.web.reactive.function.client.WebClient;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SpringBootApplication
public class RabbitMqDiscoveryClientApplication {

    public static void main(String[] args) {
        SpringApplication.run(RabbitMqDiscoveryClientApplication.class, args);
    }

}

/**
 * This requires that each broker run the RabbitMQ management plugin
 *
 * @author Josh Long
 */

class RabbitMqDiscoverClient implements DiscoveryClient {

    private final static String DESCRIPTION = "RabbitMQ-backed " + DiscoveryClient.class.getName() + " implementation";
    private static final Log log = LogFactory.getLog(RabbitMqDiscoverClient.class);
    private final String managementHost;
    private final ObjectMapper objectMapper;
    private final Map<String, ServiceInstance> registry = new ConcurrentHashMap<>();
    private final WebClient webClient;
    private final String instanceId;
    private final int managementPort = 15672;

    RabbitMqDiscoverClient(String instanceId, ObjectMapper objectMapper, WebClient.Builder builder, String managementHost, String managementUsername, String managementPassword) {
        this.objectMapper = objectMapper;
        this.instanceId = instanceId;
        this.managementHost = managementHost;
        var authentication = ExchangeFilterFunctions.basicAuthentication(managementUsername, managementPassword);
        this.webClient = builder.filter(authentication).build();
    }

    @Override
    public String description() {
        return DESCRIPTION;
    }

    @Override
    public List<ServiceInstance> getInstances(String serviceId) {
        return List.of(this.registry.get(serviceId));
    }

    @Override
    public List<String> getServices() {
        // todo the schema for the url should be pluggable
        var url = "http://" + this.managementHost + ":" + this.managementPort + "/api/queues/{vh}";
        var params = Map.of("vh", "");/// todo parameterize the virtual host and port
        var json = this.webClient.get().uri(url, params).retrieve().bodyToFlux(String.class).blockFirst();
        var names = resolveQueueNames(json)
                .stream()
                .filter(Conventions::isServiceName)
                .map(Conventions::resolveServiceName)
                .toList();
        var resetMap = new HashMap<String, ServiceInstance>();
        for (var n : names) {
            resetMap.put(n, new RabbitMqServiceInstance(this.instanceId, n, this.managementHost, this.managementPort, isSecure(url)));
        }
        synchronized (this.registry) {
            this.registry.clear();
            this.registry.putAll(resetMap);
        }
        return names;
    }

    private boolean isSecure(String url) {
        try {
            var uo = new URL(url);
            var protocol = uo.getProtocol();
            return protocol.equalsIgnoreCase("https");
        }//
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }


    private List<String> resolveQueueNames(String json) {
        try {
            var names = new ArrayList<String>();
            var rootNode = this.objectMapper.readValue(json, JsonNode.class);
            rootNode.iterator().forEachRemaining(node -> names.add(node.get("name").textValue()));
            return names;
        }//
        catch (Throwable throwable) {
            log.error("oops!", throwable);
        }
        return List.of();
    }
}

class RabbitMqServiceInstance extends DefaultServiceInstance {

    private static final String QUEUE = "queue";

    private static final String EXCHANGE = "exchange";

    public RabbitMqServiceInstance(String instanceId, String serviceId, String host, int port, boolean secure, Map<String, String> metadata) {
        super(instanceId, serviceId, host, port, secure, metadata);
    }

    public RabbitMqServiceInstance(String instanceId, String serviceId, String host, int port, boolean secure) {
        super(instanceId, serviceId, host, port, secure);
    }


    @Override
    public Map<String, String> getMetadata() {
        var supRes = super.getMetadata();
        var nm = new HashMap<String, String>();
        nm.putAll(supRes);
        var serviceId = this.getServiceId();
        nm.put(EXCHANGE, Conventions.exchangeNameForServiceName(serviceId));
        nm.put(QUEUE, Conventions.queueNameForServiceName(serviceId));
        return nm;
    }

}

@Configuration
class RabbitMqAutoRegistrationConfiguration {

    private final static Log log = LogFactory.getLog(RabbitMqAutoRegistrationConfiguration.class);

    @Bean
    ApplicationListener<ApplicationReadyEvent> test(RabbitMqDiscoverClient rabbitMqDiscoverClient) {
        return event -> rabbitMqDiscoverClient.getServices().forEach(service -> {
            log.info("service: " + service);
            rabbitMqDiscoverClient.getInstances(service).forEach(si -> System.out.println("service instance: " + si.getServiceId()));
        });
    }

    @Bean
    RabbitMqDiscoverClient rabbitMqDiscoverClient(
            Environment environment, WebClient.Builder builder, ObjectMapper objectMapper, RabbitProperties props) {
        var key = "spring.application.name";
        Assert.state(environment.containsProperty(key), "you must provide a spring.application.name");
        var name = environment.getProperty(key);
        return new RabbitMqDiscoverClient(name, objectMapper, builder, props.determineHost(),
                props.determineUsername(), props.determinePassword());
    }

    @Bean
    RabbitMqRegisteringApplicationListener serviceIsReady(AmqpAdmin admin, Environment environment) {
        return new RabbitMqRegisteringApplicationListener(admin, environment);
    }

    private static class RabbitMqRegisteringApplicationListener implements ApplicationListener<ApplicationReadyEvent> {

        private final AmqpAdmin admin;

        private final Environment environment;

        RabbitMqRegisteringApplicationListener(AmqpAdmin admin, Environment environment) {
            this.admin = admin;
            this.environment = environment;
        }

        @Override
        public void onApplicationEvent(ApplicationReadyEvent event) {
            var key = "spring.application.name";
            Assert.state(this.environment.containsProperty(key), "you must provide a spring.application.name");
            var name = this.environment.getProperty(key);
            var exchange = ExchangeBuilder.directExchange(Conventions.exchangeNameForServiceName(name)).build();
            var queue = QueueBuilder.durable(Conventions.queueNameForServiceName(name)).build();
            var binding = BindingBuilder.bind(queue).to(exchange).with(name).noargs();
            this.admin.declareExchange(exchange);
            this.admin.declareQueue(queue);
            this.admin.declareBinding(binding);
        }
    }
}

abstract class Conventions {

    private static final String PREFIX = "dc";

    static String exchangeNameForServiceName(String name) {
        return fqn(name, "exchange");
    }

    static String resolveServiceName(String fqn) {
        return isServiceName(fqn) ? fqn.substring(PREFIX.length() + 1, fqn.lastIndexOf('-')) : null;
    }

    static boolean isServiceName(String fqn) {
        return StringUtils.hasText(fqn) && fqn.startsWith(PREFIX);
    }

    static String queueNameForServiceName(String name) {
        return fqn(name, "queue");
    }

    private static String fqn(String name, String type) {
        return PREFIX + '-' + name + "-" + type;
    }
}