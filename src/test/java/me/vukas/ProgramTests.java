package me.vukas;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {ToxiProxyConfig.class, RabbitConfig.class})
@ActiveProfiles("test")
public class ProgramTests {

    @Autowired
    private RabbitTemplate rabbitTemplate;
    @Autowired
    private AckingState ackingState;

    @Test
    public void rabbitWorks() throws InterruptedException {

        Random random = new Random();
        List<Integer> numbers = new ArrayList<>();
        for(int i=0; i<500; i++){
            numbers.add(random.nextInt(100));
        }
        Set<Integer> uniqueNumbers = new HashSet<>(numbers);

        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(rabbitTemplate.getConnectionFactory());
        container.setQueueNames("outputQueue");
        final CountDownLatch messageReceived = new CountDownLatch(uniqueNumbers.size());

        container.setMessageListener(message -> {
            messageReceived.countDown();
         //   assertThat(new String(message.getBody())).isEqualTo("sdsd");
            System.out.println(new String(message.getBody()));
        });
        container.setReceiveTimeout(50);
        container.start();

        for(int i=0; i<250; i++){
            String msg = String.valueOf(numbers.get(i));
            rabbitTemplate.send("", "inputQueue1", MessageBuilder.withBody(msg.getBytes()).build());
        }

        for(int i=250; i<500; i++){
            String msg = String.valueOf(numbers.get(i));
            rabbitTemplate.send("", "inputQueue2", MessageBuilder.withBody(msg.getBytes()).build());
        }

        assertThat(messageReceived.await(60, TimeUnit.SECONDS)).isTrue();

        container.stop();

        ackingState.report();

        assertThat(ackingState.getUnacked()).isEqualTo(0);  //no unacked messages allowed
    }

}
