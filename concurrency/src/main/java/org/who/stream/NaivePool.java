package org.who.stream;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NaivePool implements EventConsumer, Closeable {
    private final EventConsumer downstream;
    private final ExecutorService executorService;

    NaivePool(int size, EventConsumer downstream) {
        this.executorService = Executors.newFixedThreadPool(size);
        this.downstream = downstream;
    }

    @Override
    public Event consume(Event event) {
        executorService.submit(() -> downstream.consume(event));
        return event;
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
    }
}
