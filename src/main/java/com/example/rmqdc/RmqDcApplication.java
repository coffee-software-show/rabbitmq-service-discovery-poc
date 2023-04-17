package com.example.rmqdc;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;
import org.springframework.web.reactive.function.client.ExchangeFilterFunctions;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.List;
import java.util.Map;

@SpringBootApplication
public class RmqDcApplication {

    public static void main(String[] args) {
        SpringApplication.run(RmqDcApplication.class, args);
    }

}


class RabbitMqDiscoverClient implements DiscoveryClient {

    private final AmqpAdmin admin;
    private final String managementUsername, managementPassword, managementHost;
    private final int managementPort;

    RabbitMqDiscoverClient(AmqpAdmin admin, int port, String managementHost, String managementUsername, String managementPassword) {
        this.admin = admin;
        this.managementUsername = managementUsername;
        this.managementPassword = managementPassword;
        this.managementPort = port;
        this.managementHost = managementHost;
    }


    @Override
    public String description() {
        return "RabbitMq-backed " + DiscoveryClient.class.getName() + " implementation";
    }

    @Override
    public List<ServiceInstance> getInstances(String serviceId) {
        return null;
    }

    @Override
    public List<String> getServices() {

        var wc = WebClient.builder()
                .filter(ExchangeFilterFunctions.basicAuthentication(this.managementUsername, this.managementPassword))
                .build();

        var url = "http://{host}:{port}/api/queues/{vh}";
        var params = Map.of("host", this.managementHost, "port", 15672, "vh", "");
        var json = wc.get().uri(url, params).retrieve()
                .bodyToFlux(String.class)
                .blockFirst();
        System.out.println("Result from RMQ: " + json);


        return List.of();
    }


    /*import requests

def rest_queue_list(user='guest', password='guest', host='localhost', port=15672, virtual_host=None):
    url = 'http://%s:%s/api/queues/%s' % (host, port, virtual_host or '')
    response = requests.get(url, auth=(user, password))
    queues = [q['name'] for q in response.json()]
    return queues
I'm using requests library in this example, but it is not significantly.

Also I found library that do it for us - pyrabbit

from pyrabbit.api import Client
cl = Client('localhost:15672', 'guest', 'guest')
queues = [q['name'] for q in cl.get_queues()]*/


}

@Configuration
class RabbitMqAutoRegistrationConfiguration {

    @Bean
    ApplicationListener <ApplicationReadyEvent> test (RabbitMqDiscoverClient rmqDc){
        return event -> {
            for (var svcId : rmqDc.getServices())
                System.out.println("svcId: " + svcId);
        };
    }

    @Bean
    RabbitMqDiscoverClient rabbitMqDiscoverClient(RabbitProperties rabbitProperties, AmqpAdmin admin) {
        return new RabbitMqDiscoverClient(admin, rabbitProperties.determinePort(),
                rabbitProperties.determineHost(), rabbitProperties.determineUsername(), rabbitProperties.determinePassword());
    }

    @Bean
    ApplicationListener<ApplicationReadyEvent> serviceIsReady(AmqpAdmin admin, Environment environment) {
        return new RabbitMqRegisteringApplicationListener(admin, environment);
    }

    static class RabbitMqRegisteringApplicationListener implements ApplicationListener<ApplicationReadyEvent> {

        private final AmqpAdmin admin;

        private final Environment environment;

        RabbitMqRegisteringApplicationListener(
                AmqpAdmin admin, Environment environment) {
            this.admin = admin;
            this.environment = environment;
        }

        @Override
        public void onApplicationEvent(ApplicationReadyEvent event) {
            var key = "spring.application.name";
            Assert.state(this.environment.containsProperty(key), "you must provide a spring.application.name");
            var name = this.environment.getProperty(key);

            var exchange = ExchangeBuilder.directExchange(Conventions.exchange(name)).build();
            var queue = QueueBuilder.durable(Conventions.queue(name)).build();
            var binding = BindingBuilder.bind(queue).to(exchange).with(name).noargs();

            this.admin.declareExchange(exchange);
            this.admin.declareQueue(queue);
            this.admin.declareBinding(binding);

        }
    }
}

abstract class Conventions {

    static String exchange(String name) {
        return name + '-' + "exchange";
    }

    static String queue(String name) {
        return name + '-' + "queue";
    }

}