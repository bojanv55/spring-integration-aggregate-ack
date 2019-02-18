package me.vukas;

import java.io.IOException;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

@TestConfiguration
public class ToxiProxyConfig {
    private static final Logger logger = LoggerFactory.getLogger(ToxiProxyConfig.class);

    @Value("${siaa.host}")
    private String siaaHost;

    @Bean
    public Network network(){
        return Network.newNetwork();
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public GenericContainer rabbitContainer(){
        return new FixedHostPortGenericContainer("rabbitmq:3.7.11-alpine")
                .withFixedExposedPort(5672, 5672)
                .withExposedPorts(5672)
                .withNetwork(network())
                .withNetworkAliases("rabbit")
                .waitingFor(Wait.forListeningPort());
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    @DependsOn("rabbitContainer")
    public GenericContainer toxiProxyContainer() {
        return new FixedHostPortGenericContainer("shopify/toxiproxy")
                .withFixedExposedPort(5673, 5673)
                .withFixedExposedPort(8474, 8474)
                .withExposedPorts(8474, 5673)
                .withNetwork(network())
                .withLogConsumer(new Slf4jLogConsumer(logger))
                .waitingFor(Wait.forLogMessage(".*8474.*\\n",1));
    }

    /**
     * toxiproxyClient talks to remote server on port 8474
     * @return
     */
    @Bean
    @DependsOn("toxiProxyContainer")
    public ToxiproxyClient toxiproxyClient(){
        return new ToxiproxyClient("127.0.0.1", 8474);
    }

    /**
     * rabbitProxy exposes locally port 5673 to connect rabbit client to and forwards to toxyProxy in docker that then
     * forwards further to rabbitMq in docker
     * @return
     * @throws IOException
     */
    @Bean
    public Proxy rabbitProxy() throws IOException {
        Proxy proxy = toxiproxyClient().createProxy("rabbit", "0.0.0.0:5673",siaaHost+":5672");
        intoxicate(proxy);
        return proxy;
    }

    private void intoxicate(Proxy proxy) throws IOException {
        proxy.toxics().bandwidth("bmdw", ToxicDirection.DOWNSTREAM, 10000);
        proxy.toxics().amqp("amqp", ToxicDirection.DOWNSTREAM);
    }
}
