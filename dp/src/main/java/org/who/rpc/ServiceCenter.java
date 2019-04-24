package org.who.rpc;

import java.io.IOException;

public class ServiceCenter implements Server {
    @Override
    public void start() {

    }

    @Override
    public void stop() throws IOException {

    }

    @Override
    public void register(Class serviceClazz, Class implClazz) {

    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public int getPort() {
        return 0;
    }
}
