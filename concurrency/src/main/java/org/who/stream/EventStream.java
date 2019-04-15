package org.who.stream;

import lombok.Value;

import java.time.Instant;
import java.util.UUID;

@Value
class Event {
    private final Instant created = Instant.now();
    private final int clientId;
    private final UUID uuid;
}

@FunctionalInterface
interface EventConsumer {
    Event consume(Event event);
}

public interface EventStream {
    void consume(EventConsumer consumer);
}