package io.smallrye.reactive.messaging.my;

import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.my.api.ConsumedMessage;
import io.smallrye.reactive.messaging.providers.MetadataInjectableMessage;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

public class MyMessage<T> implements ContextAwareMessage<T>, MetadataInjectableMessage<T> {

    private final T payload;
    private Metadata metadata;
    private final MyAckHandler ackHandler;

    private final MyFailureHandler nackHandler;
    private ConsumedMessage<?> consumed;

    public MyMessage(ConsumedMessage<T> message, MyAckHandler ackHandler, MyFailureHandler nackHandler) {
        this.consumed = message;
        this.payload = message.body();
        this.ackHandler = ackHandler;
        this.nackHandler = nackHandler;
        this.metadata = ContextAwareMessage.captureContextMetadata(new MyIncomingMetadata<>(message));
    }

    public ConsumedMessage<?> getConsumedMessage() {
        return consumed;
    }

    @Override
    public T getPayload() {
        return payload;
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public CompletionStage<Void> ack() {
        return ackHandler.handle(this);
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason, Metadata nackMetadata) {
        return nackHandler.handle(this, reason, nackMetadata);
    }

    @Override
    public void injectMetadata(Object metadataObject) {
        this.metadata = this.metadata.with(metadataObject);
    }
}
