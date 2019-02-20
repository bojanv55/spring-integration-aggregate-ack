package me.vukas;

import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@TestConfiguration
public class RabbitConfig {

    @Bean
    public Exchange exchange(){
        return ExchangeBuilder.topicExchange("outputExchange").build();
    }

    @Bean
    public Queue outputQueue12() {
        return QueueBuilder.nonDurable("outputQueue12").build();
    }

    @Bean
    public Queue outputQueue() {
        return QueueBuilder.nonDurable("outputQueue").build();
    }
/*
    @Bean
    @Primary
    @ConfigurationProperties("application.rabbitmq.first")
    public RabbitProperties firstRabbitProperties(){
        return new RabbitProperties();
    }

    @Bean
    @ConfigurationProperties("application.rabbitmq.second")
    public RabbitProperties secondRabbitProperties(){
        return new RabbitProperties();
    }
*/
  /*  @Bean
    public Binding binding(){
        return BindingBuilder.bind(outputQueue12()).to(exchange()).with("#").noargs();
    }*/

}
