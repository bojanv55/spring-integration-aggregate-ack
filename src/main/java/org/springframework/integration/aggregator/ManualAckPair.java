package org.springframework.integration.aggregator;

import java.io.IOException;

import me.vukas.AckingState;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;

public class ManualAckPair {
	private Channel channel;
	private Long deliveryTag;
	private AckingState ackingState;

	public ManualAckPair(Channel channel, Long deliveryTag, AckingState ackingState) {
		this.channel = channel;
		this.deliveryTag = deliveryTag;
		this.ackingState = ackingState;
	}

	public void basicAck(){
		try {
			this.channel.basicAck(this.deliveryTag, false);
			ackingState.removeMessage(channel.getChannelNumber(), deliveryTag);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}
