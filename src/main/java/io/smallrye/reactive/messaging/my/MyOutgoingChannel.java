package io.smallrye.reactive.messaging.my;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Flow;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.my.api.BrokerClient;
import io.smallrye.reactive.messaging.my.api.SendMessage;
import io.smallrye.reactive.messaging.my.tracing.MyOpenTelemetryInstrumenter;
import io.smallrye.reactive.messaging.my.tracing.MyTrace;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import io.smallrye.reactive.messaging.providers.helpers.SenderProcessor;
import io.smallrye.reactive.messaging.tracing.TracingUtils;
import io.vertx.mutiny.core.Vertx;

public class MyOutgoingChannel {

    private final String channel;
    private final Flow.Subscriber<? extends Message<?>> subscriber;
    private final SenderProcessor processor;
    private final BrokerClient client;
    private final String topic;
    private final boolean tracingEnabled;

    private final Instrumenter<MyTrace, Void> instrumenter;

    public MyOutgoingChannel(Vertx vertx, MyConnectorOutgoingConfiguration oc, BrokerClient client) {
        this.channel = oc.getChannel();
        this.client = client;
        this.topic = oc.getTopic().orElse(oc.getChannel());
        this.tracingEnabled = oc.getTracingEnabled();
        if (tracingEnabled) {
            this.instrumenter = MyOpenTelemetryInstrumenter.createInstrumenter(false);
        } else {
            this.instrumenter = null;
        }
        this.processor = new SenderProcessor(oc.getMaxPendingMessages(), oc.getWaitForWriteCompletion(),
                m -> publishMessage(client, m));
        this.subscriber = MultiUtils.via(processor, m -> m.onFailure().invoke(f -> {
            // log the failure
        }));
    }

    private Uni<Void> publishMessage(BrokerClient client, Message<?> message) {
        // construct the outgoing message
        SendMessage sendMessage;
        Object payload = message.getPayload();
        if (payload instanceof SendMessage) {
            sendMessage = (SendMessage) message.getPayload();
        } else {
            sendMessage = new SendMessage();
            sendMessage.setPayload(payload);
            sendMessage.setTopic(topic);
            message.getMetadata(MyOutgoingMetadata.class).ifPresent(out -> {
                sendMessage.setTopic(out.getTopic());
                sendMessage.setKey(out.getKey());
                //...
            });
        }
        if (tracingEnabled) {
            Map<String, String> properties = new HashMap<>();
            TracingUtils.traceOutgoing(instrumenter, message, new MyTrace.Builder()
                    .withProperties(properties)
                    .withTopic(sendMessage.getTopic())
                    .build());
            sendMessage.setProperties(properties);
        }
        return Uni.createFrom().completionStage(() -> client.send(sendMessage))
                .onItem().transformToUni(receipt -> Uni.createFrom().completionStage(message.ack()))
                .onFailure().recoverWithUni(t -> Uni.createFrom().completionStage(message.nack(t)));
    }

    public Flow.Subscriber<? extends Message<?>> getSubscriber() {
        return this.subscriber;
    }

    public String getChannel() {
        return this.channel;
    }
}
