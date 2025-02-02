package io.smallrye.reactive.messaging.my;

import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.my.api.BrokerClient;

public class MyFailureHandler {

    private final BrokerClient client;

    static MyFailureHandler create(BrokerClient client) {
        return new MyFailureHandler(client);
    }

    public MyFailureHandler(BrokerClient client) {
        this.client = client;
    }

    public CompletionStage<Void> handle(MyMessage<?> msg, Throwable reason, Metadata metadata) {
        return Uni.createFrom().completionStage(() -> client.reject(msg.getConsumedMessage(), reason.getMessage()))
                .emitOn(msg::runOnMessageContext)
                .subscribeAsCompletionStage();
    }
}
