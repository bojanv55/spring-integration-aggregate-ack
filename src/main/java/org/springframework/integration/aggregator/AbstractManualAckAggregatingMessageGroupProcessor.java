package org.springframework.integration.aggregator;

import com.rabbitmq.client.Channel;
import me.vukas.AckingState;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.integration.store.MessageGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.springframework.amqp.support.AmqpHeaders.PREFIX;

public abstract class AbstractManualAckAggregatingMessageGroupProcessor extends AbstractAggregatingMessageGroupProcessor {
    public static final String MANUAL_ACK_PAIRS = PREFIX + "manualAckPairs";
    private AckingState ackingState;

    public AbstractManualAckAggregatingMessageGroupProcessor(AckingState ackingState){
        this.ackingState = ackingState;
    }

    @Override
    protected Map<String, Object> aggregateHeaders(MessageGroup group) {
        Map<String, Object> aggregatedHeaders = super.aggregateHeaders(group);
        List<ManualAckPair> manualAckPairs = new ArrayList<>();
        group.getMessages().forEach(m -> {
            Channel channel = (Channel)m.getHeaders().get(AmqpHeaders.CHANNEL);
            Long deliveryTag = (Long)m.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
            manualAckPairs.add(new ManualAckPair(channel, deliveryTag, ackingState));
        });
        aggregatedHeaders.put(MANUAL_ACK_PAIRS, manualAckPairs);
        return aggregatedHeaders;
    }
}
