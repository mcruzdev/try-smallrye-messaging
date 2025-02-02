package io.smallrye.reactive.messaging.my;

import java.util.concurrent.CompletionStage;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.my.api.BrokerClient;

public class MyAckHandler {

    private final BrokerClient client;

    static MyAckHandler create(BrokerClient client) {
        return new MyAckHandler(client);
    }

    public MyAckHandler(BrokerClient client) {
        this.client = client;
    }

    public CompletionStage<Void> handle(MyMessage<?> msg) {
        return Uni.createFrom().completionStage(client.ack(msg.getConsumedMessage()))
                .emitOn(msg::runOnMessageContext)
                .subscribeAsCompletionStage();
    }
}
