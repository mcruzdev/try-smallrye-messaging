package io.smallrye.reactive.messaging.my.test;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.my.MyConnector;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class MyConnectorTest extends WeldTestBase {

    @Test
    void incomingChannel() {
        String host = "";
        int port = 0;
        String myTopic = UUID.randomUUID().toString();
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.topic", myTopic)
                .with("mp.messaging.incoming.data.client-id", "my-client")
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("mp.messaging.incoming.data.connector", MyConnector.CONNECTOR_NAME);
        MyApp app = runApplication(config, MyApp.class);

        int expected = 10;
        // TODO produce expected number of messages to myTopic

        // TODO remove assertThrows
        Assertions.assertThrows(org.awaitility.core.ConditionTimeoutException.class, () -> {
            //  wait until app received
            await().atMost(3, TimeUnit.SECONDS).until(() -> app.received().size() == expected);
        });
    }

    @ApplicationScoped
    public static class MyApp {

        List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        void consume(String msg) {
            received.add(msg);
        }

        public List<String> received() {
            return received;
        }
    }
}
