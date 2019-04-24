package org.who.rpc;

import java.io.IOException;

public interface Server {
    void start() throws IOException;

    void stop() throws IOException;

    void register(Class serviceClazz, Class implClazz);

    boolean isRunning();

    int getPort();
}
