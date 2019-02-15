package org.springframework.integration.aggregator;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import net.jodah.typetools.TypeResolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;

public class InOutLambdaCorrelationStrategy implements CorrelationStrategy {
	private final Logger logger = LoggerFactory.getLogger(InOutLambdaCorrelationStrategy.class);
	private Map<Class<?>, Function<Object, String>> typeToFunction = new HashMap<>();

	public <T extends Function<?, String>> void addFunction(T function) {
		this.addFunctionWithType(function);
	}

	@SuppressWarnings("unchecked")
	private <T extends Function<?, String>> void addFunctionWithType(T function) {
		Class<?>[] types = TypeResolver.resolveRawArguments(Function.class, function.getClass());
		this.typeToFunction.put(types[0], (Function<Object, String>)function);
	}

	@Override
	public Object getCorrelationKey(Message<?> message) {
		Class payloadType = message.getPayload().getClass();
		if (this.typeToFunction.containsKey(payloadType)) {
			return this.typeToFunction.get(payloadType).apply(message.getPayload());
		}
		else{
			logger.warn(String.format("Cannot find correlator for incoming object of %s", payloadType));
		}
		return null;
	}
}
