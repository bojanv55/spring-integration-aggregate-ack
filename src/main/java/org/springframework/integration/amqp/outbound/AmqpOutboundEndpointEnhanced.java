package org.springframework.integration.amqp.outbound;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ReturnCallback;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.Lifecycle;
import org.springframework.integration.amqp.support.MappingUtils;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * Adapter that converts and sends Messages to an AMQP Exchange.
 *
 * @author Mark Fisher
 * @author Oleg Zhurakousky
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.1
 */
public class AmqpOutboundEndpointEnhanced extends AbstractAmqpOutboundEndpoint
		implements RabbitTemplate.ConfirmCallback, ReturnCallback {

	private final AmqpTemplate amqpTemplate;

	private volatile boolean expectReply;

	public AmqpOutboundEndpointEnhanced(AmqpTemplate amqpTemplate) {
		Assert.notNull(amqpTemplate, "amqpTemplate must not be null");
		this.amqpTemplate = amqpTemplate;
		if (amqpTemplate instanceof RabbitTemplate) {
			setConnectionFactory(((RabbitTemplate) amqpTemplate).getConnectionFactory());
		}
	}

	public void setExpectReply(boolean expectReply) {
		this.expectReply = expectReply;
	}


	@Override
	public String getComponentType() {
		return this.expectReply ? "amqp:outbound-gateway" : "amqp:outbound-channel-adapter";
	}

	@Override
	protected void endpointInit() {
		if (getConfirmCorrelationExpression() != null) {
			Assert.isInstanceOf(RabbitTemplate.class, this.amqpTemplate,
					"RabbitTemplate implementation is required for publisher confirms");
			((RabbitTemplate) this.amqpTemplate).setConfirmCallback(this);
		}
		if (getReturnChannel() != null) {
			Assert.isInstanceOf(RabbitTemplate.class, this.amqpTemplate,
					"RabbitTemplate implementation is required for publisher confirms");
			((RabbitTemplate) this.amqpTemplate).setReturnCallback(this);
		}
	}

	@Override
	protected void doStop() {
		if (this.amqpTemplate instanceof Lifecycle) {
			((Lifecycle) this.amqpTemplate).stop();
		}
	}

	@Override
	protected Object handleRequestMessage(Message<?> requestMessage) {
		CorrelationData correlationData = generateCorrelationData(requestMessage);
		String exchangeName = generateExchangeName(requestMessage);
		String routingKey = generateRoutingKey(requestMessage);
		if (this.expectReply) {
			return this.sendAndReceive(exchangeName, routingKey, requestMessage, correlationData);
		}
		else {
			this.send(exchangeName, routingKey, requestMessage, correlationData);
			return null;
		}
	}

	private void send(String exchangeName, String routingKey,
			final Message<?> requestMessage, CorrelationData correlationData) {
		if (this.amqpTemplate instanceof RabbitTemplate) {
			MessageConverter converter = ((RabbitTemplate) this.amqpTemplate).getMessageConverter();
			org.springframework.amqp.core.Message amqpMessage = MappingUtils.mapMessage(requestMessage, converter,
					getHeaderMapper(), getDefaultDeliveryMode(), isHeadersMappedLast());
			addDelayProperty(requestMessage, amqpMessage);
			((RabbitTemplate)this.amqpTemplate).invoke(t -> {
				t.send(exchangeName, routingKey, amqpMessage, correlationData);
				t.waitForConfirmsOrDie(15_000);	//15 seconds wait time
				return true;
			});
		}
		else {
			this.amqpTemplate.convertAndSend(exchangeName, routingKey, requestMessage.getPayload(),
					message -> {
						getHeaderMapper().fromHeadersToRequest(requestMessage.getHeaders(),
								message.getMessageProperties());
						return message;
					});
		}
	}

	private AbstractIntegrationMessageBuilder<?> sendAndReceive(String exchangeName, String routingKey,
			Message<?> requestMessage, CorrelationData correlationData) {
		Assert.isInstanceOf(RabbitTemplate.class, this.amqpTemplate,
				"RabbitTemplate implementation is required for publisher confirms");
		MessageConverter converter = ((RabbitTemplate) this.amqpTemplate).getMessageConverter();
		org.springframework.amqp.core.Message amqpMessage = MappingUtils.mapMessage(requestMessage, converter,
				getHeaderMapper(), getDefaultDeliveryMode(), isHeadersMappedLast());
		addDelayProperty(requestMessage, amqpMessage);
		org.springframework.amqp.core.Message amqpReplyMessage =
				((RabbitTemplate) this.amqpTemplate).sendAndReceive(exchangeName, routingKey, amqpMessage,
						correlationData);

		if (amqpReplyMessage == null) {
			return null;
		}
		return buildReply(converter, amqpReplyMessage);
	}

	@Override
	public void confirm(CorrelationData correlationData, boolean ack, String cause) {
		handleConfirm(correlationData, ack, cause);
	}

	@Override
	public void returnedMessage(org.springframework.amqp.core.Message message, int replyCode, String replyText,
			String exchange, String routingKey) {
		// safe to cast; we asserted we have a RabbitTemplate in doInit()
		MessageConverter converter = ((RabbitTemplate) this.amqpTemplate).getMessageConverter();
		Message<?> returned = buildReturnedMessage(message, replyCode, replyText, exchange,
				routingKey, converter);
		getReturnChannel().send(returned);
	}

}

