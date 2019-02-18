package me.vukas;

import org.springframework.amqp.core.*;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@TestConfiguration
public class RabbitConfig {

    @Bean
    public Exchange exchange(){
        return ExchangeBuilder.topicExchange("outputExchange").build();
    }

    @Bean
    public Queue outputQueue() {
        return QueueBuilder.nonDurable("outputQueue").build();
    }

    @Bean
    public Binding binding(){
        return BindingBuilder.bind(outputQueue()).to(exchange()).with("#").noargs();
    }

}
