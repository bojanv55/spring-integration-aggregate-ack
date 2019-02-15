package org.springframework.integration.aggregator;

import java.io.IOException;

import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;

public class ManualAckPair {
	private final org.slf4j.Logger logger = LoggerFactory.getLogger(ManualAckPair.class);
	private Channel channel;
	private Long deliveryTag;

	public ManualAckPair(Channel channel, Long deliveryTag) {
		this.channel = channel;
		this.deliveryTag = deliveryTag;
	}

	public void basicAck(){
		try {
			this.channel.basicAck(this.deliveryTag, false);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}
