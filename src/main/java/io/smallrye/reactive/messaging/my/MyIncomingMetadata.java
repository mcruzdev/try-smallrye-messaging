package io.smallrye.reactive.messaging.my;

import java.util.Map;

import io.smallrye.reactive.messaging.my.api.ConsumedMessage;

public class MyIncomingMetadata<T> {

    private final ConsumedMessage<T> msg;

    public MyIncomingMetadata(ConsumedMessage<T> msg) {
        this.msg = msg;
    }

    public ConsumedMessage<T> getCustomMessage() {
        return msg;
    }

    public T getBody() {
        return msg.body();
    }

    public String getKey() {
        return msg.key();
    }

    public long getTimestamp() {
        return msg.timestamp();
    }

    public Map<String, String> getProperties() {
        return msg.properties();
    }
}
