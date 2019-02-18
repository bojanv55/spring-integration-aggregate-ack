package me.vukas;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.aggregator.*;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.amqp.outbound.AmqpOutboundEndpoint;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Transformers;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.handler.advice.ErrorMessageSendingRecoverer;
import org.springframework.integration.store.MessageGroupStore;
import org.springframework.integration.store.SimpleMessageStore;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.springframework.integration.aggregator.AbstractManualAckAggregatingMessageGroupProcessor.MANUAL_ACK_PAIRS;

@Configuration
public class FlowConfig {
    @Bean
    public Queue queue1() {
        return QueueBuilder.nonDurable("inputQueue1").build();
    }

    @Bean
    public Queue queue2() {
        return QueueBuilder.nonDurable("inputQueue2").build();
    }

    @Bean
    public MessageChannel amqpInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel manualNackChannel() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel manualAckChannel() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow queue1Flow() {
        return IntegrationFlows.from(queue1InboundAdapter())
                .wireTap(wtChannel())
                .transform(Transformers.objectToString())
                .channel(amqpInputChannel())
                .get();
    }

    @Bean
    public IntegrationFlow queue2Flow() {
        return IntegrationFlows.from(queue2InboundAdapter())
                .wireTap(wtChannel())
                .transform(Transformers.objectToString())
                .channel(amqpInputChannel())
                .get();
    }

    @Bean
    public MessageChannel wtChannel() {
        return new DirectChannel();
    }

    @ServiceActivator(inputChannel = "wtChannel")
    public void wtLog(@Header(AmqpHeaders.CHANNEL) Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) Long tag) {
        ackingState().addMessage(channel.getChannelNumber(), tag);
    }

    @Bean
    public IntegrationFlow aggregatingFlow() {
        return IntegrationFlows
                .from(amqpInputChannel())
                .aggregate(a -> a  //if declared as AggregatingMessageHandler @Bean, we can use handle()
                        .outputProcessor(messageProcessor())
                        .messageStore(messageGroupStore())
                        .correlationStrategy(correlationStrategy())
                        .expireGroupsUponCompletion(true)
                        .sendPartialResultOnExpiry(true)
                        .groupTimeout(TimeUnit.SECONDS.toMillis(10))
                        .releaseStrategy(new TimeoutCountSequenceSizeReleaseStrategy(200, TimeUnit.SECONDS.toMillis(10)))
                )
                .handle(amqpOutboundEndpoint())
                .get();
    }

    @Bean
    public Aggregatable aggregator() {
        return new Aggregatable(){

            @Override
            public String correlate(String in) {
                return in;
            }

            @Override
            public void aggregate(StringContainer out, String in) {
                out.aggregateWith(in);
            }
        };
    }

    @Bean
    public AckingState ackingState(){
        return new AckingState();
    }

    @Bean
    public CorrelationStrategy correlationStrategy() {
        InOutLambdaCorrelationStrategy correlationStrategy = new InOutLambdaCorrelationStrategy();
        Function<String, String> correlator1 = (in) -> aggregator().correlate(in);
        correlationStrategy.addFunction(correlator1);
        return correlationStrategy;
    }

    @Bean
    public MessageGroupProcessor messageProcessor() {
        InOutLambdaMessageGroupProcessor<StringContainer> processor = new InOutLambdaMessageGroupProcessor<>(StringContainer.class, ackingState());
        BiConsumer<StringContainer, String> aggregator1 = (out, in) -> aggregator().aggregate(out, in);
        processor.addConsumer(aggregator1);
        return processor;
    }

    @Bean
    public MessageGroupStore messageGroupStore() {
        return new SimpleMessageStore();
    }

    @Bean
    public RabbitTemplate ackTemplate() {
        RabbitTemplate ackTemplate = new RabbitTemplate(connectionFactory);
        ackTemplate.setMessageConverter(jackson2JsonConverter());
        return ackTemplate;
    }

    @Bean
    public MessageConverter jackson2JsonConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public PartitionMapper partitionMapper() {
        return new PartitionMapper();
    }

    private FunctionExpression<Message<? extends String>> routingKeyExpression() {
        return new FunctionExpression<>(m -> partitionMapper().mapToRoutingKey(m.getPayload()));
    }

    @Bean
    public AmqpOutboundEndpoint amqpOutboundEndpoint() {
        AmqpOutboundEndpoint outboundEndpoint = new AmqpOutboundEndpoint(ackTemplate());
        outboundEndpoint.setConfirmAckChannel(manualAckChannel());
        outboundEndpoint.setConfirmCorrelationExpressionString("#root");
        outboundEndpoint.setExchangeName("outputExchange");
        outboundEndpoint.setRoutingKeyExpression(routingKeyExpression()); //forward using patition id as routing key
        return outboundEndpoint;
    }

    @Autowired
    private ConnectionFactory connectionFactory;

    @Bean
    public SimpleMessageListenerContainer queue1ListenerContainer() {
        SimpleMessageListenerContainer listenerContainer = new SimpleMessageListenerContainer(connectionFactory);
        listenerContainer.setQueues(queue1());
        listenerContainer.setConcurrentConsumers(4);
        listenerContainer.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        listenerContainer.setConsumerTagStrategy(consumerTagStrategy());
        return listenerContainer;
    }

    @Bean
    public SimpleMessageListenerContainer queue2ListenerContainer() {
        SimpleMessageListenerContainer listenerContainer = new SimpleMessageListenerContainer(connectionFactory);
        listenerContainer.setQueues(queue2());
        listenerContainer.setConcurrentConsumers(4);
        listenerContainer.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        listenerContainer.setConsumerTagStrategy(consumerTagStrategy());
        return listenerContainer;
    }

    @Bean
    public ConsumerTagStrategy consumerTagStrategy() {
        return queue -> "aggregator." + UUID.randomUUID();
    }

    @Bean
    public AmqpInboundChannelAdapter queue1InboundAdapter() {
        AmqpInboundChannelAdapter adapter = new AmqpInboundChannelAdapter(queue1ListenerContainer());
        adapter.setRecoveryCallback(new ErrorMessageSendingRecoverer(manualNackChannel(), nackStrategy()));
        adapter.setRetryTemplate(retryTemplate());
        return adapter;
    }

    @Bean
    public AmqpInboundChannelAdapter queue2InboundAdapter() {
        AmqpInboundChannelAdapter adapter = new AmqpInboundChannelAdapter(queue2ListenerContainer());
        adapter.setRecoveryCallback(new ErrorMessageSendingRecoverer(manualNackChannel(), nackStrategy()));
        adapter.setRetryTemplate(retryTemplate());
        return adapter;
    }

    @ServiceActivator(inputChannel = "manualNackChannel")
    public void manualNack(@Header(AmqpHeaders.CHANNEL) Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) Long tag, @Payload Object payload) throws IOException {
        channel.basicAck(tag, false);	//since NACK-ing sends to dead-letter, just ACK
        ackingState().removeMessage(channel.getChannelNumber(), tag);
    }

    @ServiceActivator(inputChannel = "manualAckChannel")
    public void manualAck(@Header(MANUAL_ACK_PAIRS) List<ManualAckPair> manualAckPairs) {
        manualAckPairs.forEach(ManualAckPair::basicAck);
    }

    private ErrorMessageStrategy nackStrategy() {
        return (throwable, attributes) -> {
            Message inputMessage = (Message)attributes.getAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY);
            return new ErrorMessage(throwable, inputMessage.getHeaders());
        };
    }

    @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate template = new RetryTemplate();
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(10);
        backOffPolicy.setMaxInterval(5000);
        backOffPolicy.setMultiplier(4);
        template.setBackOffPolicy(backOffPolicy);
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(4);
        template.setRetryPolicy(retryPolicy);
        return template;
    }
}
