package org.springframework.integration.aggregator;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import net.jodah.typetools.TypeResolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.store.MessageGroup;
import org.springframework.messaging.Message;

public class InOutLambdaMessageGroupProcessor<I> extends AbstractManualAckAggregatingMessageGroupProcessor {
	private final Logger logger = LoggerFactory.getLogger(InOutLambdaMessageGroupProcessor.class);
	private Class<I> outType;
	private Map<Class<?>, BiConsumer<I, Object>> typeToConsumer = new HashMap<>();

	public InOutLambdaMessageGroupProcessor(Class<I> outType) {
		this.outType = outType;
	}

	public <T extends BiConsumer<I, ?>> void addConsumer(T consumer) {
		this.addConsumerWithType(consumer);
	}

	@SuppressWarnings("unchecked")
	private <T extends BiConsumer<I, ?>> void addConsumerWithType(T consumer) {
		Class<?>[] types = TypeResolver.resolveRawArguments(BiConsumer.class, consumer.getClass());
		this.typeToConsumer.put(types[1], (BiConsumer<I, Object>)consumer);
	}

	@Override
	protected Object aggregatePayloads(MessageGroup group, Map<String, Object> defaultHeaders) {
		I newOutType = null;
		try {
			newOutType = this.outType.newInstance();
		}
		catch (InstantiationException | IllegalAccessException e) {
			logger.error(e.getMessage(), e);
		}
		for (Message message : group.getMessages()) {
			Class payloadType = message.getPayload().getClass();
			if (this.typeToConsumer.containsKey(payloadType)) {
				this.typeToConsumer.get(payloadType).accept(newOutType, message.getPayload());
			}
			else{
				logger.warn(String.format("Cannot find aggregator for incoming object of %s", payloadType));
			}
		}
		return newOutType;
	}
}
