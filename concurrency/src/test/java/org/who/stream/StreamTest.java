package org.who.stream;

import org.junit.Test;

public class StreamTest {
    @Test
    public void consumTest() {
        EventStream es = new EventStream() {
            @Override
            public void consume(EventConsumer consumer) {
                //consumer.consume(new Event());
            }
        };
        //es.consume(new ClientProjection());
    }
}
