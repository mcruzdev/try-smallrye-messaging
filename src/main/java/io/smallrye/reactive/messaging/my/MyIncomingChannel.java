package io.smallrye.reactive.messaging.my;

import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.my.api.BrokerClient;
import io.smallrye.reactive.messaging.my.api.ConsumedMessage;
import io.smallrye.reactive.messaging.my.tracing.MyOpenTelemetryInstrumenter;
import io.smallrye.reactive.messaging.my.tracing.MyTrace;
import io.smallrye.reactive.messaging.tracing.TracingUtils;
import io.vertx.core.impl.VertxInternal;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

public class MyIncomingChannel {

    private final String channel;
    private final BrokerClient client;
    private final Context context;
    private final MyAckHandler ackHandler;
    private final MyFailureHandler failureHandler;
    private final boolean tracingEnabled;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private Flow.Publisher<? extends Message<?>> stream;

    public MyIncomingChannel(Vertx vertx, MyConnectorIncomingConfiguration cfg, BrokerClient client) {
        // create and configure the client with MyConnectorIncomingConfiguration
        this.channel = cfg.getChannel();
        this.client = client;
        this.context = Context.newInstance(((VertxInternal) vertx.getDelegate()).createEventLoopContext());
        this.ackHandler = MyAckHandler.create(this.client);
        this.failureHandler = MyFailureHandler.create(this.client);
        this.tracingEnabled = cfg.getTracingEnabled();
        Multi<? extends Message<?>> receiveMulti = Multi.createBy().repeating()
                .uni(() -> Uni.createFrom().completionStage(this.client == null ? null : this.client.poll()))
                .until(__ -> closed.get())
                .filter(Objects::nonNull)
                .emitOn(context::runOnContext)
                .map(consumed -> new MyMessage<>(consumed, ackHandler, failureHandler));

        Instrumenter<MyTrace, Void> instrumenter = MyOpenTelemetryInstrumenter.createInstrumenter(true);
        if (tracingEnabled) {
            receiveMulti = receiveMulti.map(message -> {
                ConsumedMessage<?> consumedMessage = message.getMetadata(MyIncomingMetadata.class).get().getCustomMessage();
                return TracingUtils.traceIncoming(instrumenter, message, new MyTrace.Builder()
                        .withClientId(consumedMessage.clientId())
                        .withTopic(consumedMessage.topic())
                        .withProperties(consumedMessage.properties())
                        .build());
            });
        }
        this.stream = receiveMulti;
    }

    public String getChannel() {
        return channel;
    }

    public Flow.Publisher<? extends Message<?>> getStream() {
        return this.stream;
    }

    public void close() {
        closed.compareAndSet(false, true);
        client.close();
    }

    void isReady(HealthReport.HealthReportBuilder healthReportBuilder) {
        // TODO implement
    }

}
